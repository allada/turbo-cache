{
  description = "nativelink";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-utils.url = "github:numtide/flake-utils";
    git-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    nix2container = {
      # TODO(aaronmondal): Remove this once it lands in nix2container main.
      url = "github:nlewo/nix2container/80e34774e5904aae7a3542bc3b70dab1fe048587";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.follows = "flake-utils";
    };
  };

  outputs = inputs @ {
    self,
    flake-parts,
    crane,
    rust-overlay,
    nix2container,
    ...
  }:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = [
        "x86_64-linux"
        "x86_64-darwin"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      imports = [
        inputs.git-hooks.flakeModule
        ./local-remote-execution/flake-module.nix
      ];
      perSystem = {
        config,
        pkgs,
        system,
        ...
      }: let
        stable-rust-version = "1.79.0";
        nightly-rust-version = "2024-07-24";

        # TODO(aaronmondal): Make musl builds work on Darwin.
        # See: https://github.com/TraceMachina/nativelink/issues/751
        stable-rust =
          if pkgs.stdenv.isDarwin
          then pkgs.rust-bin.stable.${stable-rust-version}
          else pkgs.pkgsMusl.rust-bin.stable.${stable-rust-version};
        nightly-rust =
          if pkgs.stdenv.isDarwin
          then pkgs.rust-bin.nightly.${nightly-rust-version}
          else pkgs.pkgsMusl.rust-bin.nightly.${nightly-rust-version};

        # TODO(aaronmondal): Tools like rustdoc don't work with the `pkgsMusl`
        # package set because of missing libgcc_s. Fix this upstream and use the
        # `stable-rust` toolchain in the devShell as well.
        # See: https://github.com/oxalica/rust-overlay/issues/161
        stable-rust-native = pkgs.rust-bin.stable.${stable-rust-version};

        maybeDarwinDeps = pkgs.lib.optionals pkgs.stdenv.isDarwin [
          pkgs.darwin.apple_sdk.frameworks.Security
          pkgs.libiconv
        ];

        llvmPackages = pkgs.llvmPackages_18;

        customStdenv = pkgs.callPackage ./tools/toolchains/llvmStdenv.nix {inherit llvmPackages;};

        # TODO(aaronmondal): This doesn't work with rules_rust yet.
        # Tracked in https://github.com/TraceMachina/nativelink/issues/477.
        customClang = pkgs.callPackage ./tools/toolchains/customClang.nix {stdenv = customStdenv;};

        nixSystemToRustTriple = nixSystem:
          {
            "x86_64-linux" = "x86_64-unknown-linux-musl";
            "aarch64-linux" = "aarch64-unknown-linux-musl";
            "x86_64-darwin" = "x86_64-apple-darwin";
            "aarch64-darwin" = "aarch64-apple-darwin";
          }
          .${nixSystem}
          or (throw "Unsupported Nix system: ${nixSystem}");

        # Calling `pkgs.pkgsCross` changes the host and target platform to the
        # cross-target but leaves the build platform the same as pkgs.
        #
        # For instance, calling `pkgs.pkgsCross.aarch64-multiplatform` on an
        # `x86_64-linux` host sets `host==target==aarch64-linux` but leaves the
        # build platform at `x86_64-linux`.
        #
        # On aarch64-darwin the same `pkgs.pkgsCross.aarch64-multiplatform`
        # again sets `host==target==aarch64-linux` but now with a build platform
        # of `aarch64-darwin`.
        #
        # For optimal cache reuse of different crosscompilation toolchains we
        # take our rust toolchain from the host's `pkgs` and remap the rust
        # target to the target platform of the `pkgsCross` target. This lets us
        # reuse the same executables (for instance rustc) to build artifacts for
        # different target platforms.
        stableRustFor = p:
          p.rust-bin.stable.${stable-rust-version}.default.override {
            targets = [
              "${nixSystemToRustTriple p.stdenv.targetPlatform.system}"
            ];
          };

        craneLibFor = p: (crane.mkLib p).overrideToolchain stableRustFor;

        src = pkgs.lib.cleanSourceWith {
          src = (craneLibFor pkgs).path ./.;
          filter = path: type:
            (builtins.match "^.*(data/SekienSkashita\.jpg|nativelink-config/README\.md)" path != null)
            || ((craneLibFor pkgs).filterCargoSources path type);
        };

        # Warning: The different usages of `p` and `pkgs` are intentional as we
        # use crosscompilers and crosslinkers whose packagesets collapse with
        # the host's packageset. If you change this, take care that you don't
        # accidentally explode the global closure size.
        commonArgsFor = p: let
          isLinuxBuild = p.stdenv.buildPlatform.isLinux;
          isLinuxTarget = p.stdenv.targetPlatform.isLinux;
          targetArch = nixSystemToRustTriple p.stdenv.targetPlatform.system;

          # Full path to the linker for CARGO_TARGET_XXX_LINKER
          linkerPath =
            if isLinuxBuild && isLinuxTarget
            then "${pkgs.mold}/bin/ld.mold"
            else "${pkgs.llvmPackages_latest.lld}/bin/ld.lld";
        in
          {
            inherit src;
            stdenv =
              if isLinuxTarget
              then p.pkgsMusl.stdenv
              else p.stdenv;
            strictDeps = true;
            buildInputs =
              [p.cacert]
              ++ pkgs.lib.optionals p.stdenv.targetPlatform.isDarwin [
                p.darwin.apple_sdk.frameworks.Security
                p.libiconv
              ];
            nativeBuildInputs =
              (
                if isLinuxBuild
                then [pkgs.mold]
                else [pkgs.llvmPackages_latest.lld]
              )
              ++ pkgs.lib.optionals p.stdenv.targetPlatform.isDarwin [
                p.darwin.apple_sdk.frameworks.Security
                p.libiconv
              ];
            CARGO_BUILD_TARGET = targetArch;
          }
          // (
            if isLinuxTarget
            then
              {
                CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static";
              }
              // (
                if linkerPath != null
                then {
                  "CARGO_TARGET_${pkgs.lib.toUpper (pkgs.lib.replaceStrings ["-"] ["_"] targetArch)}_LINKER" = linkerPath;
                }
                else {}
              )
            else {}
          );

        # Additional target for external dependencies to simplify caching.
        cargoArtifactsFor = p: (craneLibFor p).buildDepsOnly (commonArgsFor p);

        nativelinkFor = p:
          (craneLibFor p).buildPackage ((commonArgsFor p)
            // {
              cargoArtifacts = cargoArtifactsFor p;
            });

        nativeTargetPkgs =
          if pkgs.system == "x86_64-linux"
          then pkgs.pkgsCross.musl64
          else if pkgs.system == "aarch64-linux"
          then pkgs.pkgsCross.aarch64-multiplatform-musl
          else pkgs;

        nativelink = nativelinkFor nativeTargetPkgs;

        # These two can be built by all build platforms. This is not true for
        # darwin targets which are only buildable via native compilation.
        nativelink-aarch64-linux = nativelinkFor pkgs.pkgsCross.aarch64-multiplatform-musl;
        nativelink-x86_64-linux = nativelinkFor pkgs.pkgsCross.musl64;

        nativelink-debug = (craneLibFor pkgs).buildPackage ((commonArgsFor pkgs)
          // {
            cargoArtifacts = cargoArtifactsFor pkgs;
            CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static --cfg tokio_unstable";
            CARGO_PROFILE = "smol";
            cargoExtraArgs = "--features enable_tokio_console";
          });

        # Tests
        local-image-test = pkgs.callPackage ./tools/test/local-image.nix {};
        nativelink-is-executable-test = pkgs.callPackage ./tools/test/nativelink-is-executable.nix {inherit nativelink;};

        rbe-configs-gen = pkgs.callPackage ./local-remote-execution/rbe-configs-gen.nix {};
        generate-toolchains = pkgs.callPackage ./tools/toolchains/generate.nix {inherit rbe-configs-gen;};

        buck2-toolchain = import ./tools/buildsystem/buck2.nix {
          inherit pkgs;
          inherit buildImage;
          inherit self;
        };

        native-cli = pkgs.callPackage ./native-cli/default.nix {};

        docs = pkgs.callPackage ./tools/docs.nix {rust = stable-rust.default;};

        inherit (nix2container.packages.${system}.nix2container) pullImage;
        inherit (nix2container.packages.${system}.nix2container) buildImage;

        # Images
        publish-ghcr = pkgs.callPackage ./tools/images/publish-ghcr.nix {};
        # TODO(aaronmondal): Allow "crosscompiling" this image. At the moment
        #                    this would set a wrong container architecture. See:
        #                    https://github.com/nlewo/nix2container/issues/138.
        nativelink-image = import ./tools/images/nativelink.nix {
          inherit self;
          inherit buildImage;
          inherit pkgs;
          inherit nativelink-x86_64-linux;
          inherit nativelink-aarch64-linux;
        };

        siso-chromium = import ./tools/images/siso-chromium.nix {
          inherit buildImage;
          inherit pullImage;
        };

        toolchain-drake = import ./tools/images/toolchain-drake.nix {
          inherit buildImage;
          inherit pullImage;
        };

        nativelink-worker-init = pkgs.callPackage ./tools/nativelink/worker-init.nix {inherit buildImage self nativelink-image;};
        createWorker = pkgs.callPackage ./tools/nativelink/create-worker.nix {inherit buildImage self;};

        # LRE
        rbe-autogen = pkgs.callPackage ./local-remote-execution/rbe-autogen.nix {
          inherit buildImage;
          stdenv = customStdenv;
        };

        lre-cc = pkgs.callPackage ./local-remote-execution/lre-cc.nix {
          inherit customClang buildImage;
          stdenv = customStdenv;
        };
      in rec {
        _module.args.pkgs = let
          nixpkgs-patched = import ./tools/patches/apply.nix {
            inherit self;
            inherit system;
          };
        in
          import nixpkgs-patched {
            inherit system;
            overlays = [(import rust-overlay)];
          };
        apps = {
          default = {
            type = "app";
            program = "${nativelink}/bin/nativelink";
          };
          native = {
            type = "app";
            program = "${native-cli}/bin/native";
          };
        };
        packages =
          rec {
            inherit
              local-image-test
              lre-cc
              native-cli
              nativelink
              nativelink-aarch64-linux
              nativelink-debug
              nativelink-image
              nativelink-is-executable-test
              nativelink-worker-init
              nativelink-x86_64-linux
              publish-ghcr
              ;
            default = nativelink;

            rbe-autogen-lre-cc = rbe-autogen lre-cc;
            nativelink-worker-lre-cc = createWorker lre-cc;
            lre-java = pkgs.callPackage ./local-remote-execution/lre-java.nix {inherit buildImage;};
            rbe-autogen-lre-java = rbe-autogen lre-java;
            nativelink-worker-lre-java = createWorker lre-java;
            nativelink-worker-siso-chromium = createWorker siso-chromium;
            nativelink-worker-toolchain-drake = createWorker toolchain-drake;
            nativelink-worker-buck2-toolchain = buck2-toolchain;
            image = nativelink-image;
          }
          // (
            # It's not possible to crosscompile to darwin, not even between
            # x86_64-darwin and aarch64-darwin. We create these targets anyways
            # To keep them uniform with the linux targets if they're buildable.
            if pkgs.stdenv.system == "aarch64-darwin"
            then {
              nativelink-aarch64-darwin = nativelink;
            }
            else if pkgs.stdenv.system == "x86_64-darwin"
            then {
              nativelink-x86_64-darwin = nativelink;
            }
            else {}
          );
        checks = {
          # TODO(aaronmondal): Fix the tests.
          # tests = craneLib.cargoNextest (commonArgs
          #   // {
          #   inherit cargoArtifacts;
          #   cargoNextestExtraArgs = "--all";
          #   partitions = 1;
          #   partitionType = "count";
          # });
        };
        pre-commit.settings = {
          hooks = import ./tools/pre-commit-hooks.nix {
            inherit pkgs nightly-rust;
          };
        };
        local-remote-execution.settings = {
          Env =
            if pkgs.stdenv.isDarwin
            then [] # Doesn't support Darwin yet.
            else lre-cc.meta.Env;
          prefix = "lre";
        };

        devShells.default = import ./tools/shell/base.nix {
          inherit config;
          inherit stable-rust-native;
          inherit pkgs;
          inherit local-image-test;
          inherit generate-toolchains;
          inherit customClang;
          inherit native-cli;
          inherit docs;
          inherit maybeDarwinDeps;
        };

        # Example for another devShell called "test"
        # devShells.test = import ./tools/shell/base.nix { inherit pkgs; };
      };
    }
    // {
      flakeModule = ./local-remote-execution/flake-module.nix;
    };
}
