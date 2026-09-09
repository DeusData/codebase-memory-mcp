import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";

const packageJson = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
);
const forbiddenDependencies = [
  "@react-three/drei",
  "@react-three/fiber",
  "@react-three/postprocessing",
  "postprocessing",
  "three",
];
for (const dependency of forbiddenDependencies) {
  if (packageJson.dependencies?.[dependency] || packageJson.devDependencies?.[dependency]) {
    throw new Error(`diagnostic dashboard must not depend on ${dependency}`);
  }
}

const removedExplorerSources = [
  "src/components/GraphScene.tsx",
  "src/components/GraphTab.tsx",
  "src/components/NodeCloud.tsx",
  "src/hooks/useGraphData.ts",
];
for (const source of removedExplorerSources) {
  if (existsSync(new URL(`../${source}`, import.meta.url))) {
    throw new Error(`diagnostic dashboard still contains explorer source: ${source}`);
  }
}

const assetsDir = new URL("../dist/assets/", import.meta.url);
const initialChunk = readdirSync(assetsDir)
  .find((name) => /^index-.*\.js$/.test(name));

if (!initialChunk) {
  throw new Error("initial dashboard chunk was not found; run the production build first");
}

const maxBytes = 220 * 1024;
const bytes = statSync(new URL(initialChunk, assetsDir)).size;

if (bytes > maxBytes) {
  throw new Error(
    `initial dashboard bundle is ${(bytes / 1024).toFixed(1)} KiB; budget is ${maxBytes / 1024} KiB`,
  );
}

console.log(`initial dashboard bundle: ${(bytes / 1024).toFixed(1)} KiB (budget: ${maxBytes / 1024} KiB)`);
