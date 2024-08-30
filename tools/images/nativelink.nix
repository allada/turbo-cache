{
  self,
  buildImage,
  pkgs,
  nativelink-x86_64-linux,
  nativelink-aarch64-linux,
}: let
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
  }
