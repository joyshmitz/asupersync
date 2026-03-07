import fs from "node:fs";
import path from "node:path";

const repo = process.cwd();
const distDir = path.join(repo, "dist");
const indexHtml = path.join(distDir, "index.html");
const assetsDir = path.join(distDir, "assets");

if (!fs.existsSync(indexHtml)) {
  throw new Error(`missing built index.html: ${indexHtml}`);
}

if (!fs.existsSync(assetsDir)) {
  throw new Error(`missing built assets directory: ${assetsDir}`);
}

const assetEntries = fs.readdirSync(assetsDir).filter((entry) => entry.endsWith(".js"));
if (assetEntries.length === 0) {
  throw new Error("missing built JavaScript asset in dist/assets");
}

const indexHtmlContent = fs.readFileSync(indexHtml, "utf8");
if (!indexHtmlContent.includes("/assets/")) {
  throw new Error("built index.html does not reference hashed assets");
}

console.log(
  JSON.stringify(
    {
      status: "ok",
      indexHtml,
      jsAssets: assetEntries,
    },
    null,
    2,
  ),
);
