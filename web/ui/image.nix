{
  pkgs,
  buildImage,
  pullImage,
  ...
}: let
  # Base image configuration for nginx:mainline-alpine-slim
  imageParams = {
    imageName = "nginx";
    imageDigest = "sha256:e9293c9bedb0db866e7d2b69e58131db4c2478e6cd216cdd99b134830703983a";
    sha256 = "sha256:63f36d95235a84d401bd3e061ffb0c25f72ca1d7b0ad154e583b7e25479d424f";
    description = "Nginx mainline Alpine slim image for serving Vue3 applications.";
    title = "Nginx Alpine Slim for Vue3";
  };

  # Derivation to build the Vue3 web UI using Bun
  webUi = pkgs.stdenv.mkDerivation {
    pname = "nativelink-web-ui";
    version = "1.0.0"; # Update as necessary

    src = ./.; # Path to your Vue3 project

    buildInputs = [pkgs.bun];

    # Ensure Bun is available in the build environment
    buildPhase = ''
      bun install
      bun run build
    '';

    # Install the built files and nginx configuration
    installPhase = ''
            mkdir -p $out/dist
            cp -r dist/* $out/dist/

            mkdir -p $out/etc/nginx
            cat > $out/etc/nginx/nginx.conf <<EOF
      events {}
      http {
        server {
          listen 80;
          server_name localhost;

          root /dist;
          index index.html;

          location / {
            try_files \$uri \$uri/ /index.html;
          }
        }
      }
      EOF
    '';

    # Specify that only the /dist and /etc/nginx directories are output
    meta = {
      description = "Builds the Vue3 application and prepares Nginx configuration.";
    };
  };
in
  buildImage {
    name = "nativelink-ui";

    # Base image configuration using nginx:mainline-alpine-slim
    fromImage = pullImage {
      inherit (imageParams) imageName imageDigest sha256;
      tlsVerify = true;
      os = "linux";
    };

    # Container configuration
    config = {
      # Set the working directory to /dist where the built files are located
      WorkingDir = "/dist";

      # Define the entrypoint to start Nginx in the foreground
      Entrypoint = ["nginx" "-g" "daemon off;"];

      # Expose port 80 for HTTP traffic
      ExposedPorts = {
        "80/tcp" = {};
      };

      # Define volumes to copy built files and nginx configuration
      Volumes = {
        "/dist" = "${webUi}/dist";
        "/etc/nginx/nginx.conf" = "${webUi}/etc/nginx/nginx.conf";
      };

      # Add descriptive labels
      Labels = {
        "org.opencontainers.image.description" = imageParams.description;
        "org.opencontainers.image.title" = imageParams.title;
      };
    };
  }
