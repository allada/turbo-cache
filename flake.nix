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
    };
    nix2container = {
      # TODO(SchahinRohani): Use a specific commit hash until nix2container is stable.
      url = "github:nlewo/nix2container/cc96df7c3747c61c584d757cfc083922b4f4b33e";
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
        ./tools/darwin/flake-module.nix
        ./tools/nixos/flake-module.nix
        ./flake-module.nix
      ];
      perSystem = {
        config,
        pkgs,
        system,
        ...
      }: let
        stable-rust-version = "1.82.0";
        nightly-rust-version = "2024-11-23";

        # TODO(aaronmondal): Tools like rustdoc don't work with the `pkgsMusl`
        # package set because of missing libgcc_s. Fix this upstream and use the
        # `stable-rust` toolchain in the devShell as well.
        # See: https://github.com/oxalica/rust-overlay/issues/161
        stable-rust-native = pkgs.rust-bin.stable.${stable-rust-version};
        nightly-rust-native = pkgs.rust-bin.nightly.${nightly-rust-version};

        maybeDarwinDeps = pkgs.lib.optionals pkgs.stdenv.isDarwin [
          pkgs.darwin.apple_sdk.frameworks.CoreFoundation
          pkgs.darwin.apple_sdk.frameworks.Security
          pkgs.libiconv
        ];

        llvmPackages = pkgs.llvmPackages_19;

        customStdenv = pkgs.callPackage ./tools/llvmStdenv.nix {inherit llvmPackages;};

        # TODO(aaronmondal): This doesn't work with rules_rust yet.
        # Tracked in https://github.com/TraceMachina/nativelink/issues/477.
        customClang = pkgs.callPackage ./tools/customClang.nix {stdenv = customStdenv;};

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

        nightlyRustFor = p:
          p.rust-bin.nightly.${nightly-rust-version}.default.override {
            extensions = ["llvm-tools"];
            targets = [
              "${nixSystemToRustTriple p.stdenv.targetPlatform.system}"
            ];
          };

        craneLibFor = p: (crane.mkLib p).overrideToolchain stableRustFor;
        nightlyCraneLibFor = p: (crane.mkLib p).overrideToolchain nightlyRustFor;

        src = pkgs.lib.cleanSourceWith {
          src = (craneLibFor pkgs).path ./.;
          filter = path: type:
            (builtins.match "^.*(data/SekienAkashita\.jpg|nativelink-config/README\.md)" path != null)
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
            else "${llvmPackages.lld}/bin/ld.lld";
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
                else [llvmPackages.lld]
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
        nightlyCargoArtifactsFor = p: (craneLibFor p).buildDepsOnly (commonArgsFor p);

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

        publish-ghcr = pkgs.callPackage ./tools/publish-ghcr.nix {};

        local-image-test = pkgs.callPackage ./tools/local-image-test.nix {};

        nativelink-is-executable-test = pkgs.callPackage ./tools/nativelink-is-executable-test.nix {inherit nativelink;};

        rbe-configs-gen = pkgs.callPackage ./local-remote-execution/rbe-configs-gen.nix {};

        generate-toolchains = pkgs.callPackage ./tools/generate-toolchains.nix {inherit rbe-configs-gen;};

        native-cli = pkgs.callPackage ./native-cli/default.nix {};

        build-chromium-tests =
          pkgs.writeShellScriptBin
          "build-chromium-tests"
          ./deploy/chromium-example/build_chromium_tests.sh;

        docs = pkgs.callPackage ./tools/docs.nix {rust = stable-rust-native.default;};

        inherit (nix2container.packages.${system}.nix2container) pullImage;
        inherit (nix2container.packages.${system}.nix2container) buildImage;

        # TODO(aaronmondal): Allow "crosscompiling" this image. At the moment
        #                    this would set a wrong container architecture. See:
        #                    https://github.com/nlewo/nix2container/issues/138.
        nativelink-image = let
          nativelinkForImage =
            if pkgs.stdenv.isx86_64
            then nativelink-x86_64-linux
            else nativelink-aarch64-linux;
        in
          buildImage {
            name = "nativelink";
            copyToRoot = [
              (pkgs.buildEnv {
                name = "nativelink-buildEnv";
                paths = [nativelinkForImage];
                pathsToLink = ["/bin"];
              })
            ];
            config = {
              Entrypoint = [(pkgs.lib.getExe' nativelinkForImage "nativelink")];
              Labels = {
                "org.opencontainers.image.description" = "An RBE compatible, high-performance cache and remote executor.";
                "org.opencontainers.image.documentation" = "https://github.com/TraceMachina/nativelink";
                "org.opencontainers.image.licenses" = "Apache-2.0";
                "org.opencontainers.image.revision" = "${self.rev or self.dirtyRev or "dirty"}";
                "org.opencontainers.image.source" = "https://github.com/TraceMachina/nativelink";
                "org.opencontainers.image.title" = "NativeLink";
                "org.opencontainers.image.vendor" = "Trace Machina, Inc.";
              };
            };
          };

        nativelink-worker-init = pkgs.callPackage ./tools/nativelink-worker-init.nix {inherit buildImage self nativelink-image;};

        rbe-autogen = pkgs.callPackage ./local-remote-execution/rbe-autogen.nix {
          inherit buildImage;
          stdenv = customStdenv;
        };
        createWorker = pkgs.callPackage ./tools/create-worker.nix {inherit buildImage self;};
        buck2-toolchain = let
          buck2-nightly-rust-version = "2024-04-28";
          buck2-nightly-rust = pkgs.rust-bin.nightly.${buck2-nightly-rust-version};
          buck2-rust = buck2-nightly-rust.default.override {extensions = ["rust-src"];};
        in
          pkgs.callPackage ./tools/create-worker-experimental.nix {
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
          };
        siso-chromium = buildImage {
          name = "siso-chromium";
          fromImage = pullImage {
            imageName = "gcr.io/chops-public-images-prod/rbe/siso-chromium/linux";
            imageDigest = "sha256:26de99218a1a8b527d4840490bcbf1690ee0b55c84316300b60776e6b3a03fe1";
            sha256 = "sha256-v2wctuZStb6eexcmJdkxKcGHjRk2LuZwyJvi/BerMyw=";
            tlsVerify = true;
            arch = "amd64";
            os = "linux";
          };
        };
        lre-cc = pkgs.callPackage ./local-remote-execution/lre-cc.nix {
          inherit customClang buildImage;
          stdenv = customStdenv;
        };
        toolchain-drake = buildImage {
          name = "toolchain-drake";
          # imageDigest and sha256 are generated by toolchain-drake.sh for non-reproducible builds.
          fromImage = pullImage {
            imageName = "localhost:5001/toolchain-drake";
            imageDigest = ""; # DO NOT COMMIT DRAKE IMAGE_DIGEST VALUE
            sha256 = ""; # DO NOT COMMIT DRAKE SHA256 VALUE
            tlsVerify = false;
            arch = "amd64";
            os = "linux";
          };
        };
        toolchain-buck2 = buildImage {
          name = "toolchain-buck2";
          # imageDigest and sha256 are generated by toolchain-buck2.sh for non-reproducible builds.
          fromImage = pullImage {
            imageName = "localhost:5001/toolchain-buck2";
            imageDigest = ""; # DO NOT COMMIT BUCK2 IMAGE_DIGEST VALUE
            sha256 = ""; # DO NOT COMMIT BUCK2 SHA256 VALUE
            tlsVerify = false;
            arch = "amd64";
            os = "linux";
          };
        };

        nativelinkCoverageFor = p: let
          coverageArgs =
            (commonArgsFor p)
            // {
              # TODO(aaronmondal): For some reason we're triggering an edgecase where
              #                    mimalloc builds against glibc headers in coverage
              #                    builds. This leads to nonexistend __memcpy_chk and
              #                    __memset_chk symbols if fortification is enabled.
              #                    Our regular builds also have this issue, but we
              #                    should investigate further.
              hardeningDisable = ["fortify"];
            };
        in
          (nightlyCraneLibFor p).cargoLlvmCov (coverageArgs
            // {
              cargoArtifacts = nightlyCargoArtifactsFor p;
              cargoExtraArgs = builtins.concatStringsSep " " [
                "--all"
                "--locked"
                "--features nix"
                "--branch"
                "--ignore-filename-regex '.*(genproto|vendor-cargo-deps|crates).*'"
              ];
              cargoLlvmCovExtraArgs = "--html --output-dir $out";
            });

        nativelinkCoverageForHost = nativelinkCoverageFor pkgs;
      in rec {
        _module.args.pkgs = let
          nixpkgs-patched = (import self.inputs.nixpkgs {inherit system;}).applyPatches {
            name = "nixpkgs-patched";
            src = self.inputs.nixpkgs;
            patches = [
              ./tools/nixpkgs_link_libunwind_and_libcxx.diff
              ./tools/nixpkgs_disable_ratehammering_pulumi_tests.diff
            ];
          };
          rust-overlay-patched = (import self.inputs.nixpkgs {inherit system;}).applyPatches {
            name = "rust-overlay-patched";
            src = self.inputs.rust-overlay;
            patches = [
              # This dependency has a giant dependency chain and we don't need
              # it for our usecases.
              ./tools/rust-overlay_cut_libsecret.diff
            ];
          };
        in
          import nixpkgs-patched {
            inherit system;
            overlays = [(import rust-overlay-patched)];
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
              nativelinkCoverageForHost
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
            nativelink-worker-toolchain-buck2 = createWorker toolchain-buck2;
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
            inherit pkgs;
            nightly-rust = nightly-rust-native;
          };
        };
        local-remote-execution.settings = {
          Env =
            if pkgs.stdenv.isDarwin
            then [] # Doesn't support Darwin yet.
            else lre-cc.meta.Env;
          prefix = "linux";
        };
        nixos.settings = {
          path = with pkgs; [
            "/run/current-system/sw/bin"
            "${binutils.bintools}/bin"
            "${uutils-coreutils-noprefix}/bin"
            "${customClang}/bin"
            "${git}/bin"
            "${python3}/bin"
          ];
        };
        devShells.default = pkgs.mkShell {
          nativeBuildInputs = let
            bazel = pkgs.writeShellScriptBin "bazel" ''
              unset TMPDIR TMP
              exec ${pkgs.bazelisk}/bin/bazelisk "$@"
            '';
          in
            [
              # Development tooling
              pkgs.git
              pkgs.pre-commit

              # Rust
              stable-rust-native.default
              bazel

              ## Infrastructure
              pkgs.awscli2
              pkgs.skopeo
              pkgs.dive
              pkgs.cosign
              pkgs.kubectl
              pkgs.kubernetes-helm
              pkgs.kubectx
              pkgs.cilium-cli
              pkgs.vale
              pkgs.trivy
              pkgs.docker-client
              pkgs.kind
              pkgs.tektoncd-cli
              pkgs.pulumi
              pkgs.pulumiPackages.pulumi-language-go
              pkgs.fluxcd
              pkgs.go
              pkgs.kustomize

              ## Web
              pkgs.bun # got patched to the newest version (v.1.1.25)
              pkgs.lychee
              pkgs.nodejs_22 # For pagefind search
              pkgs.playwright-driver
              pkgs.playwright-test

              # Additional tools from within our development environment.
              local-image-test
              generate-toolchains
              customClang
              native-cli
              docs
              build-chromium-tests
            ]
            ++ maybeDarwinDeps
            ++ pkgs.lib.optionals (pkgs.stdenv.system != "x86_64-darwin") [
              # Old darwin systems are incompatible with deno.
              pkgs.deno
            ];

          shellHook =
            ''
              # Generate the .pre-commit-config.yaml symlink when entering the
              # development shell.
              ${config.pre-commit.installationScript}

              # Generate lre.bazelrc which configures LRE toolchains when
              # running in the nix environment.
              ${config.local-remote-execution.installationScript}

              # Generate nativelink.bazelrc which gives Bazel invocations access
              # to NativeLink's read-only cache.
              ${config.nativelink.installationScript}

              # If on NixOS, generate nixos.bazelrc which adds the required
              # NixOS binary paths to the bazel environment.
              if [ -e /etc/nixos ]; then
                ${config.nixos.installationScript}
                export CC=customClang
              fi

              # The Bazel and Cargo builds in nix require a Clang toolchain.
              # TODO(aaronmondal): The Bazel build currently uses the
              #                    irreproducible host C++ toolchain. Provide
              #                    this toolchain via nix for bitwise identical
              #                    binaries across machines.
              export CC=clang
              export PULUMI_K8S_AWAIT_ALL=true
              export PLAYWRIGHT_BROWSERS_PATH=${pkgs.playwright-driver.browsers}
              export PLAYWRIGHT_NODEJS_PATH=${pkgs.nodePackages_latest.nodejs}
              export PATH=$HOME/.deno/bin:$PATH
              deno types > web/platform/utils/deno.d.ts
            ''
            + (pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
              # On Darwin generate darwin.bazelrc which configures
              # darwin libs & frameworks when running in the nix environment.
              ${config.darwin.installationScript}
            '');
        };
      };
    }
    // {
      flakeModule = {
        default = ./flake-module.nix;
        darwin = ./tools/darwin/flake-module.nix;
        local-remote-execution = ./local-remote-execution/flake-module.nix;
        nixos = ./tools/nixos/flake-module.nix;
      };
    };
}
