"""Independent, standard-library-only support for the live-v2 regression driver."""

import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import threading
import time

STOP = threading.Event()


class Regression(Exception):
    pass


class Infrastructure(Exception):
    pass


def require(condition, message):
    if not condition:
        raise Regression(message)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(65536), b""):
            result.update(block)
    return result.hexdigest()


def write_json(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def clean_env():
    result = os.environ.copy()
    for key in ("CBM_TEST_COVERAGE_DIR", "CBM_TEST_ONLY", "CBM_TEST_ONLY_FILE"):
        result.pop(key, None)
    return result


def identity(path, executable=False):
    target = Path(path).resolve(strict=True)
    if not target.is_file() or (executable and not os.access(target, os.X_OK)):
        raise Infrastructure("not a regular readable/executable input: " + str(target))
    return {"path": str(target), "sha256": digest(target), "bytes": target.stat().st_size}


class Processes:
    """Own only newly started process groups; preserve bounded complete command logs."""

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir()
        self.records = []
        self.total_output_bytes = 0

    @staticmethod
    def group_alive(pgid):
        try:
            os.killpg(pgid, 0)
            return True
        except ProcessLookupError:
            return False

    def run(self, label, argv, env=None, timeout=180):
        if STOP.is_set():
            raise KeyboardInterrupt("driver stop requested")
        directory = self.root / ("%03d-%s" % (len(self.records), label))
        directory.mkdir()
        record = {"label": label, "argv": [str(x) for x in argv], "timeout": timeout,
                  "stdout": str(directory / "stdout.log"),
                  "stderr": str(directory / "stderr.log"), "cleanup": []}
        self.records.append(record)
        state = {"bytes": 0, "overflow": False, "errors": []}
        lock = threading.Lock()
        stop = threading.Event()
        started = time.monotonic()
        try:
            process = subprocess.Popen(record["argv"], env=env, stdin=subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       start_new_session=True, bufsize=0)
        except OSError as exc:
            record["launch_error"] = str(exc)
            write_json(directory / "process.json", record)
            raise Infrastructure("command launch failed: " + label) from exc
        record["pid"] = process.pid

        def drain(source, destination):
            try:
                with Path(destination).open("xb") as output:
                    while True:
                        block = source.read(65536)
                        if not block:
                            break
                        with lock:
                            remaining = max(0, min(64 * 1024 * 1024 - state["bytes"],
                                                   512 * 1024 * 1024 - self.total_output_bytes))
                            state["bytes"] += len(block)
                            self.total_output_bytes += len(block)
                            if len(block) > remaining:
                                state["overflow"] = True
                                stop.set()
                        output.write(block[:remaining])
            except Exception as exc:
                with lock:
                    state["errors"].append(str(exc))
                stop.set()
            finally:
                source.close()

        threads = [threading.Thread(target=drain, args=(source, record[name]), daemon=True)
                   for source, name in ((process.stdout, "stdout"), (process.stderr, "stderr"))]
        timed_out, interrupted = False, None
        try:
            write_json(directory / "started.json", record)
            for thread in threads:
                thread.start()
            while process.poll() is None and not stop.is_set() and not STOP.is_set():
                if time.monotonic() - started >= timeout:
                    timed_out = True
                    break
                stop.wait(0.02)
        except BaseException as exc:
            interrupted = exc
        descendants_after_exit = process.poll() is not None and self.group_alive(process.pid)
        for signum in (signal.SIGTERM, signal.SIGKILL):
            if not self.group_alive(process.pid):
                break
            record["cleanup"].append(signum.name)
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                break
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                process.poll()
                if not self.group_alive(process.pid):
                    break
                time.sleep(0.02)
        quiescent = not self.group_alive(process.pid)
        for thread, source in zip(threads, (process.stdout, process.stderr)):
            if thread.ident is None:
                source.close()
            else:
                thread.join(5)
        record.update(exit=process.poll(), elapsed=time.monotonic() - started,
                      timed_out=timed_out, quiescent=quiescent,
                      descendants_after_exit=descendants_after_exit, capture=state,
                      drainers_done=all(not thread.is_alive() for thread in threads))
        write_json(directory / "process.json", record)
        if interrupted is not None:
            raise interrupted
        if STOP.is_set():
            raise KeyboardInterrupt("driver stop requested")
        if (timed_out or not quiescent or descendants_after_exit or state["overflow"]
                or state["errors"] or not record["drainers_done"]):
            raise Infrastructure("bounded process custody/capture failed: " + label)
        return record


def proftext(data):
    """Read the pinned LLVM frontend text grammar independently of project imports."""
    require(data and b"\r" not in data and b"\x00" not in data, "invalid proftext bytes")
    lines = data.split(b"\n")
    require(lines[-1] == b"", "proftext lacks final LF")
    records, position = {}, 0
    while position < len(lines) - 1:
        require(position + 6 < len(lines), "truncated proftext header")
        name, hash_label, hash_text, size_label, size_text, values_label = lines[position:position + 6]
        require(bool(name) and hash_label == b"# Func Hash:" and size_label == b"# Num Counters:"
                and values_label == b"# Counter Values:", "unsupported proftext record")
        require(re.fullmatch(rb"0|[1-9][0-9]*", hash_text) is not None,
                "noncanonical native function hash")
        require(re.fullmatch(rb"[1-9][0-9]*", size_text) is not None,
                "noncanonical native counter count")
        hash_value, count = int(hash_text), int(size_text)
        require(hash_value < 2**64 and count <= 1000000, "proftext numeric bound")
        begin, end = position + 6, position + 6 + count
        require(end < len(lines) - 1 and lines[end] == b"", "proftext record terminator")
        values = []
        for value in lines[begin:end]:
            require(re.fullmatch(rb"0|[1-9][0-9]*", value) is not None,
                    "noncanonical native counter")
            values.append(int(value))
            require(values[-1] < 2**64, "native counter overflow")
        key = (name, hash_value)
        require(key not in records, "duplicate native identity")
        records[key] = tuple(values)
        position = end + 1
    require(bool(records), "empty native universe")
    return records


def profile_wire(data, image, universe):
    expected = [b"CBM_PROFILE_MAP\t2\t" + image.encode() + b"\t" + str(len(universe)).encode()]
    ids = {}
    for number, key in enumerate(sorted(universe)):
        name, hash_value = key
        ids[key] = number
        expected.append(b"%d\t%s\t%016x\t%d" %
                        (number, name.hex().encode(), hash_value, len(universe[key])))
    require(data == b"\n".join(expected) + b"\n",
            "profiles.tsv differs from full native identities/shapes, including zero records")
    return ids


def test_rows(data):
    require(data.endswith(b"\n"), "tests.tsv final LF")
    rows, order = {}, []
    for line in data.splitlines():
        fields = line.split(b"\t")
        require(len(fields) == 4 and fields[0] not in rows, "tests.tsv row framing/uniqueness")
        key, status, reasons, raw_ids = fields
        require(status == b"incomplete", "CLI fabricated complete evidence")
        values = [] if not raw_ids else raw_ids.split(b" ")
        require(all(re.fullmatch(rb"0|[1-9][0-9]*", value) for value in values), "test ID syntax")
        ids = [int(value) for value in values]
        require(ids == sorted(set(ids)), "test IDs must be sorted/unique")
        rows[key] = (reasons.split(b";"), ids)
        require(b"row_evidence_missing" in rows[key][0], "missing unadmitted row evidence reason")
        order.append(key)
    require(order == sorted(order), "test rows not sorted")
    return rows
