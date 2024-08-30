import { defineConfig, passthroughImageService } from "astro/config";

import mdx from "@astrojs/mdx";
import sitemap from "@astrojs/sitemap";
import starlight from "@astrojs/starlight";
import deno from "@deno/astro-adapter";
// import partytown from "@astrojs/partytown";

import { rehypeHeadingIds } from "@astrojs/markdown-remark";
import { rehypeMermaid } from "@beoe/rehype-mermaid"; // "rehype-mermaid";
import tailwindcss from "@tailwindcss/vite";
import rehypeAutolinkHeadings from "rehype-autolink-headings";
import { starlightConfig } from "./starlight.conf.ts";
// import { default as playformCompress } from "@playform/compress";

// https://astro.build/config
export default defineConfig({
  site: "https://docs.nativelink.com",
  trailingSlash: "never",
  output: "hybrid",
  image: {
    service: passthroughImageService(),
  },
  adapter: deno({
    port: 8881,
    hostname: "localhost",
  }),
  markdown: {
    rehypePlugins: [
      rehypeHeadingIds,
      [
        rehypeAutolinkHeadings,
        {
          behavior: "wrap",
        },
      ],
      [
        rehypeMermaid,
        // TODO(aaronmondal): The "@beoe/cache" package doesn't build on
        // Cloudflare. Reimplement our own.
        {
          class: "not-content",
          strategy: "img-class-dark-mode",
        },
      ],
    ],
  },
  vite: {
    plugins: [tailwindcss()],
  },
  integrations: [sitemap(), starlight(starlightConfig), mdx()],
});
