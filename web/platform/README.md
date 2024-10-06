# The NativeLink documentation

The NativeLink documentation gets deployed to <https://docs.nativelink.com>.

## 📚 Stack

The NativeLink documentation uses a custom, highly efficient, high performance
stack. Getting a bunch of bleeding-edge tools to work well together can be
challenging. Feel free to copy-paste it into your own projects.

- [Diátaxis](https://diataxis.fr/) as overarching documentation philosophy.
- [Bun](https://github.com/oven-sh/bun) as production bundler.
- [Biome](https://biomejs.dev/) as linting toolchain.
- [Astro](https://astro.build/) as meta-framework.
- [Starlight](https://starlight.astro.build/de/) as documentation framework.
- [TailwindCSS 4.0-alpha](https://tailwindcss.com/blog/tailwindcss-v4-alpha) for
  component styling which makes use of [LightningCSS](https://lightningcss.dev/)
  for faster CSS processing.
- [Deno Deploy](https://deno.com/deploy) for deployments.

## 🚀 Common workflows

See [`web/platform/package.json`](https://github.com/TraceMachina/nativelink/blob/main/web/platform/package.json)
for build scripts.

This project requires `bun` and `deno`. The nix flake ships compatible versions.

```bash
# Install dependencies with bun.
bun install

# Rebuild the docs reference. (Note MacOS: Use Bun & Bazel from Host)
bun run build.docs

# Generate the simple parts of the autogenerated docs.
bun run generate.docs

# Rebuild everything. Make sure to remove the `dist` directory beforehand.
bun run build

# Serve the dist directory with deno
bun serve

# Run a development server. Doesn't rebuild the autogenerated parts of the docs.
bun dev

# Run formatter and linter checks.
bun check

# Apply formatter and linter fixes.
bun fix

# Test Deno deployments locally. Useful when debugging SSR. Rebuilds the
# autogenerated parts of the docs.
bun preview
```


## 🐛 Known issues

- `bun dev` doesn't hot reload the changes in the starlight.conf.ts