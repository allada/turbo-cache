// biome-ignore lint/correctness/noNodejsModules: Always runs ahead of time.
import { join } from "node:path";
import { generateAstroContent } from "./metaphase";
import type { Crate } from "./rustdoc_types";

export async function generateDocs(config: {
  crateDataPath: string;
  outputPath: string;
}) {
  try {
    const crateDataPath = join(import.meta.dir, config.crateDataPath);
    const crateData: Crate = JSON.parse(await Bun.file(crateDataPath).text());

    const markdownContent = generateAstroContent(crateData);

    const outputPath = join(import.meta.dir, config.outputPath);
    await Bun.write(outputPath, markdownContent);

    console.info(`Generated: ${outputPath}`);
  } catch (error) {
    console.error("An error occurred during generation:", error);
  }
}

// Only run if this file is being executed directly
if (import.meta.main) {
  await generateDocs({
    crateDataPath:
      "../../bazel-bin/nativelink-config/docs_json.rustdoc/nativelink_config.json",
    outputPath: "../src/content/docs/reference/nativelink-config.mdx",
  });
}
