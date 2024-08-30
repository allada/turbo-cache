{
  buildImage,
  pullImage,
}:
buildImage {
  name = "siso-chromium";
  fromImage = pullImage {
    imageName = "gcr.io/chops-public-images-prod/rbe/siso-chromium/linux";
    imageDigest = "sha256:26de99218a1a8b527d4840490bcbf1690ee0b55c84316300b60776e6b3a03fe1";
    sha256 = "sha256-v2wctuZStb6eexcmJdkxKcGHjRk2LuZwyJvi/BerMyw=";
    tlsVerify = true;
    arch = "amd64";
    os = "linux";
  };
}
