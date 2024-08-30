{
  pkgs,
  buildImage,
  self,
}: let
  buck2-nightly-rust-version = "2024-04-28";
  buck2-nightly-rust = pkgs.rust-bin.nightly.${buck2-nightly-rust-version};
  buck2-rust = buck2-nightly-rust.default.override {extensions = ["rust-src"];};
in
  pkgs.callPackage ../nativelink/create-worker-experimental.nix {
    inherit buildImage self;
    imageName = "buck2-toolchain";
    packagesForImage = [
      pkgs.coreutils
      pkgs.bash
      pkgs.go
      pkgs.diffutils
      pkgs.gnutar
      pkgs.gzip
      pkgs.python3Full
      pkgs.unzip
      pkgs.zstd
      pkgs.cargo-bloat
      pkgs.mold-wrapped
      pkgs.reindeer
      pkgs.lld_16
      pkgs.clang_16
      buck2-rust
    ];
  }
