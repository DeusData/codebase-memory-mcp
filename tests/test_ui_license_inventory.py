"""License decisions must describe the built browser pack, including workers."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class BrowserLicenseInventory(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.ui = Path(self.tmp.name) / "graph-ui"
        self.ui.mkdir()
        self.package("@huggingface/transformers", "4.2.0", "Apache-2.0")
        source = ROOT / "graph-ui/node_modules/@huggingface/transformers/dist/transformers.web.js"
        if not source.is_file():
            self.fail("Install graph-ui dependencies before these packaging tests")
        browser = self.ui / "node_modules/@huggingface/transformers/dist/transformers.web.js"
        browser.parent.mkdir()
        shutil.copyfile(source, browser)
        self.browser_path = str(browser.relative_to(self.ui)).replace(os.sep, "/")
        meta = self.ui / "node_modules/@huggingface/transformers/package.json"
        package = json.loads(meta.read_text())
        package["exports"] = {"default": {"default": "./dist/transformers.web.js"}}
        meta.write_text(json.dumps(package))
        (self.ui / "package-lock.json").write_text('{"lockfileVersion":3}')
        (self.ui / "src").mkdir()
        (self.ui / "src/main.ts").write_text("new Worker('worker.js');")
        (self.ui / "src/worker.ts").write_text("import '@huggingface/transformers';")
        (self.ui / "index.html").write_text("The source HTML template")
        (self.ui / "dist/assets").mkdir(parents=True)
        (self.ui / "dist/index.html").write_text('<script src="assets/main.js"></script>')
        (self.ui / "dist/assets/main.js").write_text("new Worker('worker.js');")
        (self.ui / "dist/assets/worker.js").write_text("postMessage('ready');")
        inputs = ["src/main.ts", "src/worker.ts", self.browser_path,
                  "node_modules/@huggingface/transformers/package.json"]
        inputs.append("index.html")
        self.proof = {
            "version": 1,
            "lock_sha256": digest(self.ui / "package-lock.json"),
            "inputs": {name: digest(self.ui / name) for name in inputs},
            "modules": inputs[:3] + ["index.html"], "externals": [],
            "chunks": ["assets/main.js", "assets/worker.js"],
            "worker_builds": 1,
            "outputs": {str(p.relative_to(self.ui / "dist")).replace(os.sep, "/"): digest(p)
                        for p in (self.ui / "dist").rglob("*") if p.is_file()},
            "runtime_inputs": {},
            "generated_assets": {"index.html": digest(self.ui / "dist/index.html")},
        }
        self.write_proof()
        self.tree = {"dependencies": {
            "@huggingface/transformers": {"version": "4.2.0", "dependencies": {}}}}
        self.bin = Path(self.tmp.name) / "bin"
        self.bin.mkdir()
        shim = self.bin / "npm.py"
        shim.write_text("from pathlib import Path\nprint(Path('npm-tree.json').read_text())\n")
        (self.bin / "npm").write_text("#!/usr/bin/env python3\n" + shim.read_text())
        (self.bin / "npm").chmod(0o755)
        (self.bin / "npm.cmd").write_text('@python "%~dp0npm.py"\r\n')

    def package(self, name, version, license_id, **platform):
        folder = self.ui / "node_modules" / name
        folder.mkdir(parents=True)
        (folder / "package.json").write_text(json.dumps({
            "name": name, "version": version, "license": license_id, **platform}))
        (folder / "LICENSE").write_text(license_id)

    def add_transformers_dependency(self, name, version, license_id, **platform):
        self.package(name, version, license_id, **platform)
        deps = self.tree["dependencies"]["@huggingface/transformers"]["dependencies"]
        deps[name] = {"version": version}

    def write_proof(self):
        path = self.ui / ".vite/license-build.json"
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps(self.proof))

    def run_script(self, script):
        (self.ui / "npm-tree.json").write_text(json.dumps(self.tree))
        args = [os.sys.executable, str(ROOT / "scripts" / script), str(self.ui)]
        if script == "license-gate-check-npm.py":
            args.append(str(ROOT / "scripts/license-policy.json"))
        return subprocess.run(args, capture_output=True, text=True,
                              env={**os.environ, "PATH": str(self.bin) + os.pathsep + os.environ["PATH"]})

    def assert_blocked(self):
        result = self.run_script("license-gate-check-npm.py")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_permissive_tree_with_verified_build_passes(self):
        result = self.run_script("license-gate-check-npm.py")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("all allow-listed", result.stdout)

    def test_notices_cover_the_production_tree(self):
        result = self.run_script("gen-ui-licenses.py")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("### @huggingface/transformers@4.2.0", result.stdout)

    def test_lgpl_package_blocks_even_when_unshipped(self):
        # No exception for "installed but not bundled": sharp's wasm build
        # (LGPL libvips inside) once passed that way. It must block now.
        self.add_transformers_dependency("@img/sharp-wasm32", "0.35.0",
                                         "Apache-2.0 AND LGPL-3.0-or-later AND MIT")
        result = self.run_script("license-gate-check-npm.py")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("@img/sharp-wasm32@0.35.0", result.stdout)

    def test_platform_restricted_lgpl_package_blocks(self):
        # os/cpu-restricted native packages used to be skipped as build
        # tooling; that skip let all ten @img/sharp-libvips-* (LGPL) through.
        self.add_transformers_dependency("@img/sharp-libvips-darwin-arm64", "1.3.4",
                                         "LGPL-3.0-or-later", os=["darwin"], cpu=["arm64"])
        result = self.run_script("license-gate-check-npm.py")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("@img/sharp-libvips-darwin-arm64@1.3.4", result.stdout)

    def test_platform_restricted_permissive_package_passes(self):
        # Checking platform packages must not turn into blocking them.
        self.add_transformers_dependency("@example/native-darwin-arm64", "1.0.0", "MIT",
                                         os=["darwin"], cpu=["arm64"])
        result = self.run_script("license-gate-check-npm.py")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_missing_proof_blocks(self):
        (self.ui / ".vite/license-build.json").unlink()
        self.assert_blocked()

    def test_changed_lockfile_blocks(self):
        (self.ui / "package-lock.json").write_text("{}")
        self.assert_blocked()

    def test_changed_source_blocks(self):
        (self.ui / "src/worker.ts").write_text("import 'sharp';")
        self.assert_blocked()

    def test_changed_output_blocks(self):
        (self.ui / "dist/assets/worker.js").write_text("import 'sharp';")
        self.assert_blocked()

    def test_uninventoried_output_blocks(self):
        (self.ui / "dist/assets/extra.js").write_text("// copied payload")
        self.assert_blocked()

    def test_copied_javascript_is_not_a_bundle_chunk(self):
        self.proof["chunks"].remove("assets/worker.js")
        self.write_proof()
        self.assert_blocked()

    def test_unknown_binary_blocks_even_if_hashed(self):
        path = self.ui / "dist/assets/unknown.wasm"
        path.write_bytes(b"unknown payload")
        self.proof["outputs"]["assets/unknown.wasm"] = digest(path)
        self.write_proof()
        self.assert_blocked()

    def test_copied_public_html_blocks_even_if_hashed(self):
        source = self.ui / "public/copied.html"
        source.parent.mkdir()
        source.write_text("A copied document outside the bundler graph")
        output = self.ui / "dist/copied.html"
        shutil.copyfile(source, output)
        self.proof["inputs"]["public/copied.html"] = digest(source)
        self.proof["outputs"]["copied.html"] = digest(output)
        self.write_proof()
        self.assert_blocked()

    def test_copied_public_css_blocks_even_if_hashed(self):
        source = self.ui / "public/copied.css"
        source.parent.mkdir()
        source.write_text("body { color: red; }")
        output = self.ui / "dist/assets/copied.css"
        shutil.copyfile(source, output)
        self.proof["inputs"]["public/copied.css"] = digest(source)
        self.proof["outputs"]["assets/copied.css"] = digest(output)
        self.write_proof()
        self.assert_blocked()

    def test_generated_css_matches_bundler_output(self):
        output = self.ui / "dist/assets/generated.css"
        output.write_text("body { color: green; }")
        self.proof["outputs"]["assets/generated.css"] = digest(output)
        self.proof["generated_assets"]["assets/generated.css"] = digest(output)
        self.write_proof()
        result = self.run_script("license-gate-check-npm.py")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_worker_sharp_module_blocks(self):
        self.proof["modules"].append("node_modules/@img/sharp-wasm32/lib/sharp.node")
        self.write_proof()
        self.assert_blocked()

    def test_sharp_external_blocks(self):
        self.proof["externals"].append("sharp")
        self.write_proof()
        self.assert_blocked()

    def test_node_prebundle_blocks(self):
        self.proof["modules"].append("node_modules/@huggingface/transformers/dist/transformers.node.mjs")
        self.write_proof()
        self.assert_blocked()

    def test_virtual_sharp_module_blocks(self):
        self.proof["modules"].append("\0sharp?commonjs-external")
        self.write_proof()
        self.assert_blocked()

    def test_runtime_asset_requires_matching_package_input(self):
        source = self.ui / "node_modules/onnxruntime-web/dist/runtime.wasm"
        source.parent.mkdir(parents=True)
        source.write_bytes(b"package runtime")
        output = self.ui / "dist/assets/runtime.wasm"
        output.write_bytes(b"different runtime")
        name = source.relative_to(self.ui).as_posix()
        self.proof["inputs"][name] = digest(source)
        self.proof["outputs"]["assets/runtime.wasm"] = digest(output)
        self.proof["runtime_inputs"]["assets/runtime.wasm"] = name
        self.write_proof()
        self.assert_blocked()

    def test_copied_runtime_matches_inventoried_package_input(self):
        source = self.ui / "node_modules/onnxruntime-web/dist/runtime.wasm"
        source.parent.mkdir(parents=True)
        source.write_bytes(b"package runtime")
        output = self.ui / "dist/assets/runtime.wasm"
        shutil.copyfile(source, output)
        name = source.relative_to(self.ui).as_posix()
        self.proof["inputs"][name] = digest(source)
        self.proof["outputs"]["assets/runtime.wasm"] = digest(output)
        self.proof["runtime_inputs"]["assets/runtime.wasm"] = name
        self.write_proof()
        result = self.run_script("license-gate-check-npm.py")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_missing_worker_inventory_blocks(self):
        self.proof["worker_builds"] = 0
        self.write_proof()
        self.assert_blocked()

    def test_changed_browser_entry_blocks(self):
        path = self.ui / self.browser_path
        path.write_text("export { default } from 'sharp';")
        self.proof["inputs"][self.browser_path] = digest(path)
        self.write_proof()
        self.assert_blocked()

    def test_unrelated_disallowed_dependency_still_blocks(self):
        self.package("unrelated", "1.0.0", "GPL-3.0-only")
        self.tree["dependencies"]["unrelated"] = {"version": "1.0.0"}
        self.assert_blocked()


if __name__ == "__main__":
    unittest.main()
