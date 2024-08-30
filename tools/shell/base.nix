{
  config,
  pkgs,
  stable-rust-native,
  generate-toolchains,
  customClang,
  native-cli,
  local-image-test,
  maybeDarwinDeps,
  docs,
  ...
}:
pkgs.mkShell {
  name = "baseShell";
  nativeBuildInputs = let
    bazel = pkgs.writeShellScriptBin "bazel" ''
      unset TMPDIR TMP
      exec ${pkgs.bazelisk}/bin/bazelisk "$@"
    '';
  in
    [
      # Development tooling
      pkgs.pre-commit
      bazel
      pkgs.buck2
      stable-rust-native.default

      ## Infrastructure Stack
      pkgs.awscli2
      pkgs.skopeo
      pkgs.dive
      pkgs.cosign
      pkgs.kubectl
      pkgs.kubernetes-helm
      pkgs.cilium-cli
      pkgs.vale
      pkgs.trivy
      pkgs.docker-client
      pkgs.kind
      pkgs.tektoncd-cli
      pkgs.pulumi
      pkgs.pulumiPackages.pulumi-language-go
      pkgs.go
      pkgs.kustomize

      ## Web Stack
      # pkgs.bun
      pkgs.deno
      pkgs.lychee
      pkgs.nodejs_22

      # Additional tools from within our development environment.
      local-image-test
      generate-toolchains
      customClang
      native-cli
      docs
    ]
    ++ pkgs.lib.optionals (!pkgs.stdenv.isDarwin) [
      # The docs on Mac require a manual setup outside the flake.
      pkgs.playwright-driver.browsers
    ]
    ++ maybeDarwinDeps;
  shellHook =
    ''
      # Generate the .pre-commit-config.yaml symlink when entering the
      # development shell.
      ${config.pre-commit.installationScript}

      # Generate lre.bazelrc which configures LRE toolchains when running
      # in the nix environment.
      ${config.local-remote-execution.installationScript}

      # The Bazel and Cargo builds in nix require a Clang toolchain.
      # TODO(aaronmondal): The Bazel build currently uses the
      #                    irreproducible host C++ toolchain. Provide
      #                    this toolchain via nix for bitwise identical
      #                    binaries across machines.
      export CC=clang
    ''
    + pkgs.lib.optionalString (!pkgs.stdenv.isDarwin) ''
      export PLAYWRIGHT_BROWSERS_PATH=${pkgs.playwright-driver.browsers}
      export PLAYWRIGHT_NODEJS_PATH=${pkgs.nodePackages_latest.nodejs}
      export PATH=$HOME/.deno/bin:$PATH
    '';
}
