{pkgs, ...}:
pkgs.mkShell {
  name = "testShell";
  packages = [pkgs.hello];
}
