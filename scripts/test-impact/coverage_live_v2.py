"""Experimental, bounded POSIX native transport for unadmitted profile-map v2.

Only actual observations are transported. Registry/image hashes are custody
facts, not evidence that a registry, interval, runtime or image set is complete.
No RowEvidence is created here, so this bundle cannot authorize negative
coverage evidence or test narrowing. Raw evidence is retained, including on
failure. Process groups cannot prove closure of children that leave the group.
"""

import errno
import hashlib
import json
import os
import re
import selectors
import signal
import stat
import subprocess
import sys
import time

from profile_map_v2 import (
    ExpectedSuite, ObservationRole, ProfileMapError, ProfileMapLimits,
    ProfileMapLimitExceededError, ProfileMapCancelledError,
    ProfileObservation, SuiteOutcome, build_profile_map_v2,
)
from profile_text_parser import (
    ParseLimits, ProfileTextError, LimitExceededError, ParseCancelledError, parse_profile_text,
)


REGISTRY_CAP = 2 * 1024 * 1024
ROW_CAP = 65536
RECORD_CAP = 4000000
COUNTER_CAP = 32000000
NAME_CAP = 128 * 1024 * 1024
GRACE = 2.0
PROFILE = re.compile(r"^(.+?)\.(parent|[0-9]+)\.profraw$")
SETUP = re.compile(r"^_setup\.(?:[0-9]+|child\.[0-9]+)\.profraw$")
MARKER = re.compile(r"^(.+?)\.([0-9]+)\.forked$")


class CollectionError(ValueError):
    def __init__(self, kind, message):
        super().__init__(message)
        self.kind = kind


def _error(kind, message):
    raise CollectionError(kind, message)


def _directory(path):
    """Open each absolute component without following symlinks; caller closes."""
    path = os.path.abspath(path)
    fd = os.open(os.sep, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.split(os.sep):
            if not part:
                continue
            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                              dir_fd=fd)
            os.close(fd)
            fd = next_fd
        return fd
    except BaseException:
        os.close(fd)
        raise


def _regular(path):
    parent, name = os.path.split(os.path.abspath(path))
    directory = _directory(parent)
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                     dir_fd=directory)
    finally:
        os.close(directory)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        _error("input", "input is not a regular file")
    return fd


def _signature(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _read(path, limit):
    fd = _regular(path)
    try:
        before = os.fstat(fd)
        if before.st_size > limit:
            _error("resource_limit", "file byte limit exceeded")
        data = bytearray()
        while True:
            part = os.read(fd, min(65536, limit + 1 - len(data)))
            if not part:
                break
            data.extend(part)
            if len(data) > limit:
                _error("resource_limit", "file byte limit exceeded")
        if _signature(before) != _signature(os.fstat(fd)):
            _error("identity", "input changed during capture")
        return bytes(data)
    finally:
        os.close(fd)


def _identity(path):
    fd = _regular(path)
    try:
        before = os.fstat(fd)
        if before.st_size > 1024 * 1024 * 1024:
            _error("resource_limit", "executable exceeds 1GiB identity limit")
        digest = hashlib.sha256()
        size = 0
        while True:
            part = os.read(fd, 65536)
            if not part:
                break
            size += len(part)
            if size > 1024 * 1024 * 1024:
                _error("resource_limit", "executable grew beyond identity limit")
            digest.update(part)
        if _signature(before) != _signature(os.fstat(fd)) or size != before.st_size:
            _error("identity", "executable changed while hashing")
        if not os.access(path, os.X_OK):
            _error("input", "executable is not executable")
        return {"path": path, "sha256": digest.hexdigest(), "bytes": size}
    finally:
        os.close(fd)


def _pairs(values):
    result = {}
    for key, value in values:
        if key in result:
            _error("input", "duplicate registry key")
        result[key] = value
    return result


def _identifier(value, suite=False):
    if type(value) is not str or not value:
        _error("input", "registry identifier must be a nonempty string")
    try:
        data = value.encode("utf-8", "strict")
    except UnicodeError:
        _error("input", "registry identifier is not valid UTF-8")
    if any(char in data for char in (0, 9, 10, 13)) or (suite and b":" in data):
        _error("input", "registry identifier violates producer grammar")
    if not suite and value == "*":
        _error("input", "setup test name is reserved")
    # Native routing uses each identifier as one filesystem component. The
    # pure producer accepts a wider byte grammar; those routes are unsupported.
    if value in (".", "..") or "/" in value or "\\" in value:
        _error("unsupported", "registry identifier cannot be a native path component")
    if not suite and value in ("_setup", "_setup.child"):
        _error("unsupported", "registry test collides with native setup routing")
    return data


def _registry(path, image):
    raw = _read(path, REGISTRY_CAP)
    try:
        value = json.loads(raw.decode("utf-8", "strict"), object_pairs_hook=_pairs,
                           parse_constant=lambda _: _error("input", "nonfinite JSON number"))
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        _error("input", "invalid registry JSON")
    if type(value) is not dict or set(value) != {"format", "image_sha256", "suites"}:
        _error("input", "registry keys do not match v0 schema")
    if value["format"] != "cbm.coverage.registry.v0":
        _error("input", "unsupported registry format")
    declared = value["image_sha256"]
    if type(declared) is not str or not re.fullmatch(r"[0-9a-f]{64}", declared):
        _error("input", "registry image digest must be lowercase SHA256")
    if declared != image["sha256"]:
        _error("identity", "registry does not match runner image")
    suites = value["suites"]
    if type(suites) is not list or not suites:
        _error("input", "registry suites must be nonempty")
    result = {}
    rows = 0
    for item in suites:
        if type(item) is not dict or set(item) != {"name", "tests"}:
            _error("input", "registry suite keys do not match schema")
        name = item["name"]
        _identifier(name, suite=True)
        if name in result:
            _error("input", "duplicate registry suite")
        tests = item["tests"]
        if type(tests) is not list or not tests:
            _error("input", "registry tests must be nonempty")
        seen = set()
        for test in tests:
            _identifier(test)
            if test in seen:
                _error("input", "duplicate registry test")
            seen.add(test)
        rows += 1 + len(tests)
        if rows > ROW_CAP:
            _error("resource_limit", "registry row limit exceeded")
        result[name] = tuple(tests)
    return result, hashlib.sha256(raw).hexdigest(), raw


class Evidence:
    def __init__(self, path, args):
        self.path = os.path.abspath(path)
        self.args = args
        self.records = []
        self.uncertainty = []
        self.converted_bytes = 0
        self.captured_bytes = 0
        self.allowed_suites = set()
        self.directory = None
        parent, name = os.path.split(self.path)
        if not name:
            _error("input", "output must name a fresh directory")
        fd = _directory(parent)
        try:
            os.mkdir(name, 0o700, dir_fd=fd)
            self.directory = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                     dir_fd=fd)
        finally:
            os.close(fd)
        try:
            self.directory_identity = os.fstat(self.directory)
            for name in ("evidence", "raw"):
                os.mkdir(name, 0o700, dir_fd=self.directory)
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.directory is not None:
            os.close(self.directory)
            self.directory = None

    def check(self):
        fd = _directory(self.path)
        try:
            actual = os.fstat(fd)
            if (actual.st_dev, actual.st_ino) != (self.directory_identity.st_dev,
                                                self.directory_identity.st_ino):
                _error("identity", "owned output directory was replaced")
        finally:
            os.close(fd)

    def write(self, relative, data):
        self.check()
        parent, name = os.path.split(relative)
        fd = _directory(os.path.join(self.path, parent))
        try:
            target = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=fd)
            try:
                with os.fdopen(target, "wb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
            except BaseException:
                # Partial evidence remains; no marker is created on this path.
                raise
        finally:
            os.close(fd)
        return os.path.join(self.path, relative)

    def json(self, relative, value):
        data = (json.dumps(value, sort_keys=True, indent=2,
                           ensure_ascii=True) + "\n").encode("ascii")
        if len(data) > 64 * 1024 * 1024:
            _error("resource_limit", "metadata exceeds 64MiB bound")
        return self.write(relative, data)

    def publish(self, meta):
        # A partial JSON file is never a commit marker. The pending complete
        # file remains evidence, and hard-link creation cannot overwrite a
        # preexisting or symlink destination.
        self.json("meta.pending.json", meta)
        for name in ("profiles", "tests"):
            data = _read(os.path.join(self.path, name + ".tsv"), self.args.max_map_bytes)
            if hashlib.sha256(data).hexdigest() != meta[name + "_sha256"]:
                _error("identity", "wire changed before publication")
        self.check()
        os.fsync(self.directory)
        os.link("meta.pending.json", "meta.json", src_dir_fd=self.directory,
                dst_dir_fd=self.directory, follow_symlinks=False)

    def inventory(self):
        """Bound scans while children run; never follow raw tree symlinks."""
        self.check()
        root = os.path.join(self.path, "raw")
        pending = [root]
        files, total, count = [], 0, 1  # listing counts as one profile slot
        try:
            info = os.stat("listing.profraw", dir_fd=self.directory, follow_symlinks=False)
        except FileNotFoundError:
            info = None
        if info is not None:
            if not stat.S_ISREG(info.st_mode):
                _error("input", "listing profile is not a regular file")
            if info.st_size > self.args.max_profile_bytes:
                _error("resource_limit", "listing profile byte limit exceeded")
            total = info.st_size
            if total > self.args.max_total_profile_bytes:
                _error("resource_limit", "aggregate raw byte limit exceeded")
        while pending:
            directory = pending.pop()
            fd = _directory(directory)
            try:
                with os.scandir(fd) as entries:
                    for entry in entries:
                        count += 1
                        if count > self.args.max_profile_files:
                            _error("resource_limit", "raw entry count limit exceeded")
                        info = entry.stat(follow_symlinks=False)
                        path = os.path.join(directory, entry.name)
                        if stat.S_ISDIR(info.st_mode) and directory == root:
                            if entry.name not in self.allowed_suites:
                                self.uncertainty.append({"path": path, "reason": "unknown_suite_destination"})
                                _error("input", "raw suite directory is outside requested scope")
                            pending.append(path)
                        elif stat.S_ISREG(info.st_mode):
                            if info.st_size > self.args.max_profile_bytes:
                                _error("resource_limit", "raw file byte limit exceeded")
                            total += info.st_size
                            if total > self.args.max_total_profile_bytes:
                                _error("resource_limit", "aggregate raw byte limit exceeded")
                            files.append(path)
                        else:
                            self.uncertainty.append({"path": path, "reason": "unsupported_raw_entry"})
                            _error("input", "unsupported raw tree entry")
            finally:
                os.close(fd)
        return sorted(files)


def _group_exists(pid):
    try:
        os.killpg(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _quiesce(proc):
    sent = []
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if proc.poll() is not None and not _group_exists(proc.pid):
            return True, sent
        try:
            os.killpg(proc.pid, sig)
            sent.append(signal.Signals(sig).name)
        except ProcessLookupError:
            pass
        until = time.monotonic() + GRACE
        while time.monotonic() < until:
            if proc.poll() is not None and not _group_exists(proc.pid):
                return True, sent
            time.sleep(0.02)
    return proc.poll() is not None and not _group_exists(proc.pid), sent


def _run(evidence, label, argv, environment, timeout, cwd, watch_raw=False):
    selector = selectors.DefaultSelector()
    streams = {"stdout": bytearray(), "stderr": bytearray()}
    started = time.monotonic()
    proc = None
    failure = None
    quiescent, cleanup = False, []
    try:
        proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, env=environment, cwd=cwd,
                                start_new_session=True, bufsize=0)
        for name, pipe in (("stdout", proc.stdout), ("stderr", proc.stderr)):
            os.set_blocking(pipe.fileno(), False)
            selector.register(pipe, selectors.EVENT_READ, name)
        while True:
            if watch_raw:
                evidence.inventory()
            left = timeout - (time.monotonic() - started)
            if left <= 0:
                failure = "timeout"
                break
            for key, _ in selector.select(min(left, 0.05)):
                part = os.read(key.fd, 65536)
                if not part:
                    selector.unregister(key.fileobj)
                    continue
                used = sum(len(data) for data in streams.values())
                allowed = min(evidence.args.max_output_bytes - used,
                              evidence.args.max_total_output_bytes - evidence.captured_bytes - used)
                streams[key.data].extend(part[:max(0, allowed)])
                if len(part) > allowed:
                    failure = "resource_limit"
                    break
            if failure:
                break
            if proc.poll() is not None:
                if _group_exists(proc.pid):
                    failure = "quiescence"
                    break
                if not selector.get_map():
                    break
    except CollectionError as error:
        failure = error.kind
    except (OSError, ValueError):
        failure = "process"
    finally:
        if proc is not None:
            quiescent, cleanup = _quiesce(proc)
            if not quiescent:
                failure = "quiescence"
            if cleanup and failure is None:
                failure = "quiescence"
            proc.stdout.close()
            proc.stderr.close()
        selector.close()
    record = {"label": label, "argv": argv, "returncode": proc.returncode if proc else None,
              "failure": failure, "quiescent": quiescent, "cleanup": cleanup,
              "wall_s": round(time.monotonic() - started, 6), "timeout_s": timeout}
    evidence.captured_bytes += sum(len(data) for data in streams.values())
    for name, data in streams.items():
        path = evidence.write("evidence/" + label + "." + name, bytes(data))
        record[name] = {"path": path, "bytes": len(data),
                        "sha256": hashlib.sha256(data).hexdigest(),
                        "capture": "complete" if failure is None else "bounded_prefix"}
    evidence.json("evidence/" + label + ".process.json", record)
    evidence.records.append(record)
    return record, bytes(streams["stdout"])


def _required(record):
    if record["failure"] or record["returncode"] != 0 or not record["quiescent"]:
        _error(record["failure"] or "tool", "required native command failed: " + record["label"])


def _environment():
    result = dict(os.environ)
    for name in ("CBM_TEST_COVERAGE_DIR", "CBM_TEST_ONLY", "CBM_TEST_ONLY_FILE"):
        result.pop(name, None)
    return result


def _convert(evidence, tool, path, label, environment, root, unavailable=False):
    try:
        raw = _read(path, evidence.args.max_profile_bytes)
    except OSError as error:
        if unavailable and error.errno in (errno.EACCES, errno.EPERM, errno.ENOENT):
            evidence.uncertainty.append({"path": path, "reason": "profile_unreadable"})
            return None
        raise
    snapshot = evidence.write("evidence/" + label + ".profraw", raw)
    raw_digest = hashlib.sha256(raw).hexdigest()
    record, text = _run(evidence, label, [tool, "merge", "--instr", "--text", "--failure-mode=any",
                                         "--num-threads=1", "--output=-", snapshot],
                        environment, evidence.args.conversion_timeout, root)
    record["input"] = {"path": path, "snapshot": snapshot, "sha256": raw_digest,
                       "bytes": len(raw)}
    _required(record)
    if hashlib.sha256(_read(snapshot, evidence.args.max_profile_bytes)).hexdigest() != raw_digest:
        _error("identity", "conversion input changed")
    evidence.converted_bytes += len(text)
    if evidence.converted_bytes > evidence.args.max_total_profile_bytes:
        _error("resource_limit", "aggregate converted byte limit exceeded")
    # Reject malformed transport, even though the pure producer can represent
    # unavailable/malformed observations conservatively. Unknown identities and
    # counter-shape incompatibilities are rejected by the producer's full join.
    deadline = time.monotonic() + evidence.args.reduction_timeout
    parse_profile_text(text, ParseLimits(evidence.args.max_output_bytes, RECORD_CAP,
                                         COUNTER_CAP, NAME_CAP),
                       cancel=lambda _: time.monotonic() >= deadline)
    return text


def _scope(registry, requested, available):
    selected = requested or list(registry)
    if len(set(selected)) != len(selected):
        _error("input", "duplicate requested suite")
    for name in selected:
        if name not in registry or name not in available:
            _error("input", "requested suite is absent from registry or native list")
    return selected


def _observations(evidence, tool, registry, selected, environment, root):
    observations = []
    for index, path in enumerate(evidence.inventory()):
        relative = os.path.relpath(path, os.path.join(evidence.path, "raw"))
        parts = relative.split(os.sep)
        if len(parts) != 2 or parts[0] not in selected:
            evidence.uncertainty.append({"path": path, "reason": "unknown_profile_destination"})
            _error("input", "raw profile destination is outside requested scope")
        suite, name = parts
        marker = MARKER.fullmatch(name)
        setup = SETUP.fullmatch(name)
        profile = PROFILE.fullmatch(name)
        if setup:
            test = "*"
            role = ObservationRole.CHILD if name.startswith("_setup.child.") else ObservationRole.SETUP
        elif marker:
            test = "*" if marker.group(1) == "_setup.child" else marker.group(1)
            role = ObservationRole.CHILD
        elif profile:
            test = profile.group(1)
            role = ObservationRole.PARENT if profile.group(2) == "parent" else ObservationRole.CHILD
        else:
            evidence.uncertainty.append({"path": path, "reason": "unaccounted_raw_entry"})
            _error("input", "unaccounted raw profile entry")
        if test != "*" and test not in registry[suite]:
            evidence.uncertainty.append({"path": path, "reason": "unknown_test_identity"})
            _error("input", "raw test identifier is outside expected registry")
        if marker:
            evidence.uncertainty.append({"path": path, "reason": "unaccounted_fork"})
            text = None
        else:
            text = _convert(evidence, tool, path, "profile-%06d" % index,
                            environment, root, unavailable=True)
        observations.append(ProfileObservation(suite.encode("utf-8"), test.encode("utf-8"),
                                               name.encode("utf-8"), role, text))
    return tuple(observations)


def collect(args):
    evidence = None
    try:
        if os.name != "posix" or not all(hasattr(os, name) for name in
                                         ("O_NOFOLLOW", "O_DIRECTORY", "killpg", "set_blocking")):
            _error("unsupported", "format2 requires POSIX owned process-group custody")
        for name in ("timeout", "max_output_bytes", "max_total_output_bytes", "max_profile_bytes", "max_total_profile_bytes",
                     "max_profile_files", "conversion_timeout", "reduction_timeout", "max_map_bytes"):
            value = getattr(args, name)
            if type(value) is not int or value <= 0 or value > sys.maxsize // 2:
                _error("input", "resource limits must be positive bounded integers")
        if not args.registry or not args.llvm_bin:
            _error("input", "format2 requires --registry and explicit --llvm-bin")
        runner = os.path.realpath(os.path.abspath(args.runner))
        tool = os.path.realpath(os.path.join(os.path.abspath(args.llvm_bin), "llvm-profdata"))
        root = os.path.abspath(args.root or os.path.join(os.path.dirname(__file__), "..", ".."))
        root_fd = _directory(root)
        os.close(root_fd)
        image = _identity(runner)
        tool_identity = _identity(tool)
        registry_path = os.path.abspath(args.registry)
        registry, registry_digest, registry_bytes = _registry(registry_path, image)
        # Reject explicit scope errors before any runner/tool action.
        _scope(registry, args.suite, set(registry))
        evidence = Evidence(args.out, args)
        registry_snapshot = evidence.write("evidence/registry.json", registry_bytes)
        environment = _environment()
        environment["LLVM_PROFILE_FILE"] = os.devnull
        version_record, version = _run(evidence, "tool-version", [tool, "--version"], environment,
                                       args.conversion_timeout, root)
        _required(version_record)
        if not re.search(rb"LLVM version 22\.1\.8(?:\s|$)", version):
            _error("unsupported", "only observed LLVM 22.1.8 proftext transport is supported")
        listing_path = os.path.join(evidence.path, "listing.profraw")
        listing_env = dict(environment, LLVM_PROFILE_FILE=listing_path)
        listed, listing = _run(evidence, "list-suites", [runner, "--list-suites"], listing_env,
                               args.timeout, root, watch_raw=True)
        _required(listed)
        try:
            available = listing.decode("utf-8", "strict").splitlines()
        except UnicodeError:
            _error("input", "native suite list is not UTF-8")
        if not available or any(not name for name in available) or len(set(available)) != len(available):
            _error("input", "native suite list is empty or ambiguous")
        for name in available:
            _identifier(name, suite=True)
        selected = _scope(registry, args.suite, set(available))
        evidence.allowed_suites = set(selected)
        enumeration = _convert(evidence, tool, listing_path, "enumeration", environment, root)
        evidence.inventory()
        suite_env = dict(environment, CBM_TEST_COVERAGE_DIR=os.path.join(evidence.path, "raw"))
        outcomes = []
        expected = []
        failed = False
        for index, suite in enumerate(selected):
            record, _ = _run(evidence, "suite-%06d" % index, [runner, suite], suite_env,
                             args.timeout, root, watch_raw=True)
            if record["failure"] not in (None, "timeout") or not record["quiescent"]:
                _error(record["failure"] or "quiescence", "native suite capture failed")
            outcome = (SuiteOutcome.TIMED_OUT if record["failure"] == "timeout" else
                       SuiteOutcome.PASSED if record["returncode"] == 0 else SuiteOutcome.FAILED)
            failed = failed or outcome is not SuiteOutcome.PASSED
            expected.append(ExpectedSuite(suite.encode("utf-8"),
                                           tuple(test.encode("utf-8") for test in registry[suite]), outcome))
            outcomes.append({"suite": suite, "tests": list(registry[suite]),
                             "outcome": outcome.value, "exit": record["returncode"], "process": record})
        observations = _observations(evidence, tool, registry, selected, environment, root)
        if _identity(runner) != image or _identity(tool) != tool_identity:
            _error("identity", "runner or conversion tool changed during collection")
        # No affirmative runtime/interval/image/child assertions are supplied.
        limits = ProfileMapLimits(args.max_total_profile_bytes + REGISTRY_CAP * 2, ROW_CAP,
                                  args.max_profile_files, RECORD_CAP, RECORD_CAP, COUNTER_CAP,
                                  NAME_CAP, COUNTER_CAP, args.max_map_bytes)
        deadline = time.monotonic() + args.reduction_timeout
        produced = build_profile_map_v2(bytes.fromhex(image["sha256"]), enumeration,
                                        tuple(expected), observations, (), limits,
                                        cancel=lambda _: time.monotonic() >= deadline)
        profiles_path = evidence.write("profiles.tsv", produced.profiles_tsv)
        tests_path = evidence.write("tests.tsv", produced.tests_tsv)
        meta = {"format": 2, "admission": "unverified", "image_sha256": image["sha256"],
            "registry_sha256": registry_digest, "profiles_sha256": produced.profiles_sha256.hex(),
            "tests_sha256": produced.tests_sha256.hex(), "registry": {
            "path": registry_path, "snapshot": registry_snapshot, "sha256": registry_digest,
            "format": "cbm.coverage.registry.v0", "completeness": "unverified"},
            "scope": selected,
            "image": image, "llvm_profdata": dict(tool_identity, version=version.decode("utf-8", "replace")),
            "profiles": {"path": profiles_path, "sha256": produced.profiles_sha256.hex()},
            "tests": {"path": tests_path, "sha256": produced.tests_sha256.hex()},
            "suites": outcomes, "processes": evidence.records, "uncertainty": evidence.uncertainty,
            "evidence_root": os.path.join(evidence.path, "evidence"),
            "raw_root": os.path.join(evidence.path, "raw"),
            "row_evidence": "absent", "platform": sys.platform,
            "raw_retained": True, "captured_bytes": evidence.captured_bytes,
            "converted_bytes": evidence.converted_bytes,
            "custody": {"method": "POSIX session and process group",
                        "descendant_closure": "unproved",
                        "compiler_runtime_compatibility": "unverified"},
            "limits": {name: getattr(args, name) for name in (
                "timeout", "max_output_bytes", "max_total_output_bytes", "max_profile_bytes",
                "max_total_profile_bytes", "max_profile_files", "conversion_timeout",
                "reduction_timeout", "max_map_bytes")},
            "selection_environment": {"CBM_SKIP_PERF": environment.get("CBM_SKIP_PERF")}}
        # Consumer acceptance requires this final marker AND both wire digests.
        evidence.publish(meta)
        return 1 if failed else 0
    except (CollectionError, ProfileMapError, ProfileTextError, OSError, UnicodeError,
            ValueError, MemoryError) as error:
        kind = getattr(error, "kind", "input_or_conversion")
        if isinstance(error, (ProfileMapLimitExceededError, LimitExceededError, MemoryError)):
            kind = "resource_limit"
        if isinstance(error, (ProfileMapCancelledError, ParseCancelledError)):
            kind = "reduction_timeout"
        print("coverage-map: format2 %s: %s" % (kind, str(error)), file=sys.stderr)
        if evidence is not None:
            try:
                evidence.json("failure.json", {"format": 2, "error": kind,
                                               "message": str(error), "processes": evidence.records,
                                               "uncertainty": evidence.uncertainty})
            except (OSError, ValueError, MemoryError):
                pass
        return 2
    finally:
        if evidence is not None:
            evidence.close()
