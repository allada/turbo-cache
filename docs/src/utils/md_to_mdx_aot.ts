import { transformMarkdownToMdx } from "./md_to_mdx";

async function readMarkdownFile(filePath: string): Promise<string> {
  try {
    return await Bun.file(filePath).text();
  } catch (err) {
    console.error(`Error reading file: ${err}`);
    throw err;
  }
}

async function writeMdxFile(
  outputFilePath: string,
  mdxContent: string,
): Promise<void> {
  try {
    await Bun.write(outputFilePath, mdxContent);
    console.info(`Generated: ${outputFilePath}`);
  } catch (err) {
    console.error(`Error writing file: ${err}`);
    throw err;
  }
}

async function convertMarkdownToMdx(
  filePath: string,
  outputFilePath: string,
  description: string,
  pagefind = true,
): Promise<void> {
  try {
    const markdown = await readMarkdownFile(filePath);
    const mdxContent = await transformMarkdownToMdx(
      markdown,
      description,
      pagefind,
    );
    await writeMdxFile(outputFilePath, mdxContent);
  } catch (err) {
    console.error(`Error during conversion: ${err}`);
    throw err;
  }
}

// Convert the actual files.
convertMarkdownToMdx(
  "../local-remote-execution/README.md",
  "src/content/docs/explanations/lre.mdx",
  "Local Remote Execution architecture",
);
convertMarkdownToMdx(
  "../CONTRIBUTING.md",
  "src/content/docs/contribute/guidelines.mdx",
  "NativeLink contribution guidelines",
);
convertMarkdownToMdx(
  "README.md",
  "src/content/docs/contribute/docs.mdx",
  "Working on documentation",
);
convertMarkdownToMdx(
  "../nativelink-config/README.md",
  "src/content/docs/config/configuration-intro.mdx",
  "NativeLink configuration guide",
);
convertMarkdownToMdx(
  "../deployment-examples/chromium/README.md",
  "src/content/docs/deployment-examples/chromium.mdx",
  "NativeLink deployment example for Chromium",
);
convertMarkdownToMdx(
  "../deployment-examples/kubernetes/README.md",
  "src/content/docs/deployment-examples/kubernetes.mdx",
  "NativeLink deployment example for Kubernetes",
);
convertMarkdownToMdx(
  "../CHANGELOG.md",
  "src/content/docs/reference/changelog.mdx",
  "NativeLink's Changelog",
  false, // Set pagefind to false for changelog
);
convertMarkdownToMdx(
  "../README.md",
  "src/content/docs/introduction/setup.mdx",
  "Get started with NativeLink",
);
