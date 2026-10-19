"""Administrative command doubles: never positive native profile evidence."""

import json
from pathlib import Path
import sys

from coverage_live_v2_native import retained_profiles
from coverage_live_v2_support import digest, require


def executable(path, body):
    require(not any(char.isspace() for char in sys.executable), "test Python shebang path contains whitespace")
    path.write_text("#!" + sys.executable + "\n" + body, encoding="utf-8")
    path.chmod(0o700)
    return path


def untouched_runner(case):
    marker = case / "runner-was-invoked"
    script = executable(case / "administrative-runner", "from pathlib import Path\n"
                        + "Path(%r).write_text('invoked\\n')\nraise SystemExit(93)\n" % str(marker))
    return script, marker


def no_marker(process, destination):
    require(process["exit"] == 2, "invalid/unsupported/resource input must exit2")
    require(not (destination / "meta.json").exists(), "invalid collection committed a success marker")


def image_mismatch(context, case):
    runner, marker = untouched_runner(case)
    image = digest(runner)
    wrong = ("1" if image[0] == "0" else "0") + image[1:]
    registry = context.registry(case, image=wrong)
    process, destination = context.collect(case, registry, runner=runner)
    no_marker(process, destination)
    require(not marker.exists(), "image mismatch executed runner before rejection")
    return {"class": "administrative", "runner_invoked": False}


def existing_output(context, case, symlink=False):
    runner, marker = untouched_runner(case)
    registry = context.registry(case, image=digest(runner))
    original = case / "caller-directory"
    original.mkdir()
    sentinel = original / "caller-data"
    sentinel.write_bytes(b"caller-owned bytes\x00unchanged\n")
    old_meta = original / "meta.json"
    old_meta.write_bytes(b'{"caller":"preexisting-marker"}\n')
    before = {path.name: digest(path) for path in original.iterdir()}
    destination = case / "bundle" if symlink else original
    if symlink:
        destination.symlink_to(original, target_is_directory=True)
    process, _ = context.collect(case, registry, runner=runner, destination=destination)
    require(process["exit"] == 2, "existing output must be rejected")
    require(not marker.exists(), "existing output rejection executed runner")
    require({path.name: digest(path) for path in original.iterdir()} == before,
            "collector changed preexisting caller data/marker")
    require(not symlink or destination.is_symlink(), "collector replaced caller symlink")
    return {"class": "administrative", "caller_bytes_preserved": True, "symlink": symlink}


def invalid_registry(context, case):
    runner, marker = untouched_runner(case)
    image = digest(runner)
    valid = {"format": "cbm.coverage.registry.v0", "image_sha256": image,
             "suites": [{"name": "coverage_setup_success", "tests": ["coverage_setup_first"]}]}
    samples = {
        "duplicate-key": ('{"format":"cbm.coverage.registry.v0","format":"cbm.coverage.registry.v0",'
                          '"image_sha256":"%s","suites":[]}' % image).encode(),
        "invalid-utf8": b"\xff",
        "oversize": b" " * (2 * 1024 * 1024 + 1),
        "extra-evidence": json.dumps(dict(valid, runtime_supported=True)).encode(),
    }
    for index, test in enumerate(("../escape", "a/b", "a\\b", ".", "..", "_setup", "_setup.child")):
        item = dict(valid, suites=[{"name": "coverage_setup_success", "tests": [test]}])
        samples["routing-%d" % index] = json.dumps(item).encode()
    for name, content in samples.items():
        directory = case / name
        directory.mkdir()
        registry = directory / "registry.json"
        registry.write_bytes(content)
        process, destination = context.collect(directory, registry, runner=runner)
        no_marker(process, destination)
    require(not marker.exists(), "invalid registry reached runner")
    linked = case / "linked-registry"
    linked.mkdir()
    target = linked / "actual.json"
    target.write_text(json.dumps(valid), encoding="utf-8")
    registry = linked / "registry.json"
    registry.symlink_to(target)
    before = digest(target)
    process, destination = context.collect(linked, registry, runner=runner)
    no_marker(process, destination)
    require(registry.is_symlink() and digest(target) == before and not marker.exists(),
            "registry symlink was followed or changed")
    return {"class": "administrative", "invalid_samples": list(samples), "symlink_preserved": True}


def bounds(context, case):
    runner, marker = untouched_runner(case)
    options = ["--max-output-bytes", "--max-total-output-bytes", "--max-profile-bytes",
               "--max-total-profile-bytes", "--max-profile-files", "--conversion-timeout",
               "--max-map-bytes", "--reduction-timeout", "--timeout"]
    for number, option in enumerate(options):
        directory = case / ("zero-%d" % number)
        directory.mkdir()
        registry = context.registry(directory, image=digest(runner))
        process, destination = context.collect(directory, registry, runner=runner, extra=[option, "0"])
        no_marker(process, destination)
    require(not marker.exists(), "invalid positive bounds reached runner")
    directory = case / "native-output-bound"
    directory.mkdir()
    registry = context.registry(directory)
    process, destination = context.collect(directory, registry, extra=["--max-output-bytes", "1"])
    no_marker(process, destination)
    directory = case / "native-file-count-bound"
    directory.mkdir()
    registry = context.registry(directory)
    process, destination = context.collect(directory, registry, extra=["--max-profile-files", "1"])
    no_marker(process, destination)
    require(len(retained_profiles(destination)) >= 2, "profile bound was not reached after real native evidence")
    return {"class": "administrative-bounds-with-native-file-control", "positive_options": options}


def converter_errors(context, case):
    results = []
    for mode in ("malformed", "unknown", "nonzero", "unsupported-version"):
        directory = case / mode
        directory.mkdir()
        tools = directory / "tools"
        tools.mkdir()
        witness = directory / "administrative-conversion-witness"
        body = """import os
from pathlib import Path
import sys
real = %r
mode = %r
witness = Path(%r)
args = sys.argv[1:]
if mode == 'unsupported-version' and '--version' in args:
    witness.write_text('administrative unsupported version\\n')
    print('LLVM version 0.0.0')
    raise SystemExit(0)
observation_conversion = False
if args and args[0] == 'merge':
    counter = witness.with_name('administrative-merge-count')
    count = int(counter.read_text()) + 1 if counter.exists() else 1
    counter.write_text(str(count))
    observation_conversion = count > 1
if observation_conversion:
    witness.write_text('administrative converter response: ' + mode + '\\n')
    if mode == 'nonzero':
        print('administrative converter failure', file=sys.stderr)
        raise SystemExit(73)
    if mode == 'malformed':
        sys.stdout.buffer.write(b'unsupported proftext response\\n')
    else:
        sys.stdout.buffer.write(b'__cbm_live_v2_test_unknown_identity__\\n# Func Hash:\\n18446744073709551615\\n# Num Counters:\\n1\\n# Counter Values:\\n1\\n\\n')
    raise SystemExit(0)
os.execv(real, [real] + args)
""" % (context.tool, mode, str(witness))
        executable(tools / "llvm-profdata", body)
        registry = context.registry(directory)
        process, destination = context.collect(directory, registry, llvm_bin=tools)
        no_marker(process, destination)
        require(witness.is_file(), "administrative converter boundary was not reached")
        results.append(mode)
    return {"class": "administrative-converter-doubles", "responses": results,
            "native_positive_evidence": False}
