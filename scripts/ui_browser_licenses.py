"""Build evidence for the UI browser pack: what ships is exactly the bundler output.

There are NO license exceptions. Every package in the production dependency
tree must carry an allow-listed license whether or not it reaches the bundle
(license-gate-check-npm.py); this module only proves the bundle itself: every
asset is inventoried bundler output, and neither sharp nor Node inference code
is in the app or worker graph. Missing evidence, new payloads or stale builds
fail closed.
"""
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys


BROWSER_ENTRY = "node_modules/@huggingface/transformers/dist/transformers.web.js"
# Transformers 4.2.0's published browser distribution contains the upstream
# empty `ignore-modules:sharp` stub. Updating it requires a new source review.
BROWSER_SHA256 = "25e0cbdf5df922996299fcd2cf835101ba979b134389a0dcc54f92022ca7e0ff"
SHARP_INPUT = re.compile(r"(^|[/\\])(sharp|@img[/\\]sharp[^/\\?]*)([/\\?]|$)")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checked_path(root, name):
    path = PurePosixPath(name)
    if not name or path.is_absolute() or ".." in path.parts or "\\" in name or ":" in name:
        raise ValueError("invalid path in browser build evidence")
    return root.joinpath(*path.parts)


def validate_browser_build(ui_dir):
    """Raise ValueError unless the current app + worker build evidence holds."""
    root = Path(ui_dir)
    try:
        proof = json.loads((root / ".vite/license-build.json").read_text())
        if proof["version"] != 1 or proof["worker_builds"] < 1:
            raise ValueError("app and worker build inventories are required")
        if proof["lock_sha256"] != sha256(root / "package-lock.json"):
            raise ValueError("browser build lockfile changed")
        inputs, outputs = proof["inputs"], proof["outputs"]
        if not inputs or not outputs:
            raise ValueError("empty browser build inventory")
        for name, expected in inputs.items():
            if sha256(checked_path(root, name)) != expected:
                raise ValueError(f"browser build input changed: {name}")
        for name in [*proof["modules"], *proof["externals"], *inputs]:
            if SHARP_INPUT.search(name.replace("\0", "/")):
                raise ValueError("Sharp code appears in the browser build graph")
            if "transformers.node." in name or "onnxruntime-node" in name:
                raise ValueError("Node inference code appears in the browser build graph")
        # A browser build must resolve this reviewed export, including in the
        # inference worker. Do not infer it from npm's dependency declarations.
        meta = json.loads((root / "node_modules/@huggingface/transformers/package.json").read_text())
        if (meta["version"] != "4.2.0" or
                meta["exports"]["default"]["default"] != "./dist/transformers.web.js" or
                BROWSER_ENTRY not in proof["modules"] or
                inputs.get(BROWSER_ENTRY) != BROWSER_SHA256):
            raise ValueError("Transformers browser export needs a new license review")
        dist = root / "dist"
        actual = {p.relative_to(dist).as_posix() for p in dist.rglob("*") if p.is_file()}
        if actual != set(outputs):
            raise ValueError("browser build asset inventory changed")
        for name, expected in outputs.items():
            path = checked_path(dist, name)
            if sha256(path) != expected:
                raise ValueError(f"browser build output changed: {name}")
            suffix = path.suffix.lower()
            # Hashing a public copy does not make it a bundler output. Bind
            # generated HTML/CSS to bytes observed in the Rollup output hook.
            generated = proof["generated_assets"].get(name)
            if suffix == ".css" and generated == expected:
                continue
            if (name == "index.html" and generated == expected and
                    "index.html" in inputs and "index.html" in proof["modules"]):
                continue
            if suffix in {".js", ".mjs"} and name in proof["chunks"]:
                continue
            # ONNX ships a prebuilt runtime module and Wasm via Vite URL
            # assets. Accept only an exact copy of the inventoried package
            # input, never a filename or extension as evidence of provenance.
            source = proof["runtime_inputs"].get(name, "")
            if (suffix in {".wasm", ".js", ".mjs"} and
                    source.startswith("node_modules/onnxruntime-web/dist/") and
                    inputs.get(source) == expected):
                continue
            raise ValueError(f"browser asset has unknown provenance: {name}")
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("browser build evidence is missing or incomplete; run npm run build") from exc


if __name__ == "__main__":
    try:
        validate_browser_build(sys.argv[1])
    except ValueError as error:
        print(f"BLOCKED: {error}", file=sys.stderr)
        sys.exit(1)
    print("OK: current application, worker, and copied-runtime build evidence verified")
