{
  self,
  system,
}:
(import self.inputs.nixpkgs {inherit system;}).applyPatches {
  name = "nixpkgs-patched";
  src = self.inputs.nixpkgs;
  # To apply a patch on a pkg, go into the nixpkgs repository
  # search for the required pkg, apply your patch on it,
  # git diff > /path/to/nativelink/tools/patches/repo_pkg.diff
  # then add your patch to the patches[].
  patches = [
    ./nixpkgs_bun.diff
    ./nixpkgs_link_libunwind_and_libcxx.diff
    ./nixpkgs_disable_ratehammering_pulumi_tests.diff
    ./nixpkgs_playwright.diff
  ];
}
