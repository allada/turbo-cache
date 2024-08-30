{
  buildImage,
  self,
  lib,
  pkgsMusl,
  nativelink-image,
}:
# This image copies a bundled `nativelink` executable to a specified location.
#
# Toolchain containers shouldn't have the `nativelink` executable built-in since
# it would make it hard to update toolchain dependencies and nativelink
# independently.
#
# Instead, use this image as initContainer to populate a shared volume in a
# Deployment. Then mount that volume into the toolchain container and set the
# Deployment's entrypoint to the mounted `nativelink` executable.
let
  copyToDestination = pkgsMusl.writeShellScriptBin "copyToDestination" ''
    cp -Lv /bin/nativelink "$@"
  '';
in
  buildImage {
    name = "nativelink-worker-init";
    # Workers should use the same version of NativeLink as the scheduler and cas
    # deployments. Use the same tag for the init image so that users don't need to
    # manage multiple tags.
    fromImage = nativelink-image;
    tag = nativelink-image.imageTag;
    copyToRoot = [pkgsMusl.coreutils];
    config = {
      Entrypoint = [(lib.getExe' copyToDestination "copyToDestination")];
      Labels = {
        "org.opencontainers.image.description" = "Init container to prepare NativeLink workers.";
        "org.opencontainers.image.documentation" = "https://github.com/TraceMachina/nativelink";
        "org.opencontainers.image.licenses" = "Apache-2.0";
        "org.opencontainers.image.revision" = "${self.rev or self.dirtyRev or "dirty"}";
        "org.opencontainers.image.source" = "https://github.com/TraceMachina/nativelink";
        "org.opencontainers.image.title" = "NativeLink worker init";
        "org.opencontainers.image.vendor" = "Trace Machina, Inc.";
      };
    };
  }
