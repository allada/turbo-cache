{
  pkgs,
  nightly-rust,
  ...
}: let
  excludes = ["^nativelink-proto/genproto" "native-cli/vendor"];
in {
  # Default hooks
  trailing-whitespace-fixer = {
    inherit excludes;
    enable = true;
    name = "trailing-whitespace";
    description = "Remove trailing whitespace";
    entry = "${pkgs.python312Packages.pre-commit-hooks}/bin/trailing-whitespace-fixer";
    types = ["text"];
  };
  end-of-file-fixer = {
    inherit excludes;
    enable = true;
    name = "end-of-file-fixer";
    description = "Remove trailing whitespace";
    entry = "${pkgs.python312Packages.pre-commit-hooks}/bin/end-of-file-fixer";
    types = ["text"];
  };
  fix-byte-order-marker = {
    inherit excludes;
    enable = true;
    name = "fix-byte-order-marker";
    entry = "${pkgs.python312Packages.pre-commit-hooks}/bin/fix-byte-order-marker";
  };
  mixed-line-ending = {
    inherit excludes;
    enable = true;
    name = "mixed-line-ending";
    entry = "${pkgs.python312Packages.pre-commit-hooks}/bin/mixed-line-ending";
    types = ["text"];
  };
  check-case-conflict = {
    inherit excludes;
    enable = true;
    name = "check-case-conflict";
    entry = "${pkgs.python312Packages.pre-commit-hooks}/bin/check-case-conflict";
    types = ["text"];
  };
  detect-private-key = {
    excludes =
      excludes
      ++ [
        # Integration testfiles not intended for production.
        "deployment-examples/docker-compose/example-do-not-use-in-prod-key.pem"
        "deployment-examples/kubernetes/base/example-do-not-use-in-prod-key.pem"
      ];
    enable = true;
    name = "detect-private-key";
    entry = "${pkgs.python312Packages.pre-commit-hooks}/bin/detect-private-key";
    types = ["text"];
  };
  forbid-binary-files = {
    excludes = [
      # Landing page image for the website.
      "nativelink-docs/static/img/hero-dark.png"

      # Testdata for fastcdc.
      "nativelink-util/tests/data/SekienAkashita.jpg"

      # Buns binary lockfile
      "docs/bun.lockb"
    ];
    enable = true;
    types = ["binary"];
    entry = let
      script = pkgs.writeShellScriptBin "forbid-binary-files" ''
        set -eu

        if [ $# -gt 0 ]; then
          for filename in "''${@}"; do
            printf "[\033[31mERROR\033[0m] Found binary file: ''${filename}"
          done
          exit 1
        fi
      '';
    in "${script}/bin/forbid-binary-files";
  };

  # Dockerfile
  hadolint.enable = true;

  # Documentation
  vale = {
    inherit excludes;
    enable = true;
    settings.configPath = ".vale.ini";
  };

  # Go
  gci = {
    excludes = ["native-cli/vendor"];
    enable = true;
    name = "gci";
    entry = "${pkgs.gci}/bin/gci write";
    description = "Fix go imports.";
    types = ["go"];
  };
  gofumpt = {
    excludes = ["native-cli/vendor"];
    enable = true;
    name = "gofumpt";
    entry = "${pkgs.gofumpt}/bin/gofumpt -w -l";
    description = "Format Go.";
    types = ["go"];
  };
  golines = {
    excludes = ["native-cli/vendor"];
    enable = true;
    name = "golines";
    entry = "${pkgs.golines}/bin/golines --max-len=80 -w";
    description = "Shorten Go lines.";
    types = ["go"];
  };
  # TODO(aaronmondal): This linter works in the nix developmen environment, but
  #                    not with `nix flake check`. It's unclear how to fix this.
  golangci-lint-in-shell = {
    enable = true;
    entry = let
      script = pkgs.writeShellScript "precommit-golangci-lint" ''
        # TODO(aaronmondal): This linter works in the nix development
        #                    environment, but not with `nix flake check`. It's
        #                    unclear how to fix this.
        if [ ''${IN_NIX_SHELL} = "impure" ]; then
          export PATH=${pkgs.go}/bin:$PATH
          cd native-cli
          ${pkgs.golangci-lint}/bin/golangci-lint run --modules-download-mode=readonly
        fi
      '';
    in
      builtins.toString script;
    types = ["go"];
    require_serial = true;
    pass_filenames = false;
  };

  # Nix
  alejandra.enable = true;
  statix.enable = true;
  deadnix.enable = true;

  # Rust
  rustfmt = {
    enable = true;
    packageOverrides.cargo = nightly-rust.cargo;
    packageOverrides.rustfmt = nightly-rust.rustfmt;
  };

  # Starlark
  bazel-buildifier-format = {
    enable = true;
    name = "buildifier format";
    description = "Format Starlark";
    entry = "${pkgs.bazel-buildtools}/bin/buildifier -lint=fix";
    types = ["bazel"];
  };
  bazel-buildifier-lint = {
    enable = true;
    name = "buildifier lint";
    description = "Lint Starlark";
    entry = "${pkgs.bazel-buildtools}/bin/buildifier -lint=warn";
    types = ["bazel"];
  };
}
