{
  stdenv,
  overrideCC,
  useMoldLinker,
  llvmPackages,
}: let
  llvmToolchain =
    overrideCC (
      llvmPackages.libcxxStdenv.override {
        targetPlatform.useLLVM = true;
      }
    )
    llvmPackages.clangUseLLVM;

  toolchain =
    if stdenv.isDarwin
    then llvmToolchain # Mold doesn't support darwin.
    else useMoldLinker llvmToolchain;
in
  # This toolchain uses Clang as compiler, Mold as linker, libc++ as C++
  # standard library and compiler-rt as compiler runtime. Resulting rust
  # binaries depend dynamically linked on the nixpkgs distribution of glibc.
  # C++ binaries additionally depend dynamically on libc++, libunwind and
  # libcompiler-rt. Due to a bug we also depend on libgcc_s.
  #
  # TODO(aaronmondal): At the moment this toolchain is only used for the Cargo
  # build. The Bazel build uses a different mostly hermetic LLVM toolchain. We
  # should merge the two by generating the Bazel cc_toolchain from this stdenv.
  # This likely requires a rewrite of
  # https://github.com/bazelbuild/bazel-toolchains as the current implementation
  # has poor compatibility with custom container images and doesn't support
  # generating toolchain configs from image archives.
  #
  # TODO(aaronmondal): Due to various issues in the nixpkgs LLVM toolchains
  # we're not getting a pure Clang/LLVM toolchain here. My guess is that the
  # runtimes were not built with the degenerate LLVM toolchain but with the
  # regular GCC stdenv from nixpkgs.
  #
  # For instance, outputs depend on libgcc_s since libcxx seems to have been was
  # built with a GCC toolchain. We're also not using builtin atomics, or at
  # least we're redundantly linking libatomic.
  #
  # Fix this as it fixes a large number of issues, including better
  # cross-platform compatibility, reduced closure size, and
  # static-linking-friendly licensing. This requires building the llvm project
  # with the correct multistage bootstrapping process.
  toolchain
