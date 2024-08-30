{
  bazel_7,
  writeShellScriptBin,
  rbe-configs-gen,
}:
writeShellScriptBin "generate-toolchains" ''
  set -xeuo pipefail

  SRC_ROOT=$(git rev-parse --show-toplevel)/local-remote-execution

  cd "''${SRC_ROOT}"

  LRE_CC_IMAGE_TAG=$(nix eval .#lre-cc.imageTag --raw)

  nix run .#rbe-autogen-lre-cc.copyTo \
    docker-daemon:rbe-autogen-lre-cc:''${LRE_CC_IMAGE_TAG} -L

  ${rbe-configs-gen}/bin/rbe_configs_gen \
    --toolchain_container=rbe-autogen-lre-cc:''${LRE_CC_IMAGE_TAG} \
    --exec_os=linux \
    --target_os=linux \
    --bazel_version=${bazel_7.version} \
    --output_src_root=''${SRC_ROOT} \
    --output_config_path=generated-cc \
    --generate_java_configs=false \
    --generate_cpp_configs=true \
    --bazel_path=${bazel_7}/bin/bazel \
    --cpp_env_json=cpp_env.json

  # The rbe_configs_gen tool automatically sets the exec_properties of the
  # generated platform to the generator container name and tag. For efficiency
  # reasons the actual deployment won't be the same as this generator
  # container, so we modify this in the generated configuration.
  sed -i \
    's|rbe-autogen-lre-cc|lre-cc|g' \
    ''${SRC_ROOT}/generated-cc/config/BUILD

  chmod 644 \
    ''${SRC_ROOT}/generated-cc/LICENSE \
    ''${SRC_ROOT}/generated-cc/config/BUILD \

  LRE_JAVA_IMAGE_TAG=$(nix eval .#lre-java.imageTag --raw)

  nix run .#rbe-autogen-lre-java.copyTo \
    docker-daemon:rbe-autogen-lre-java:''${LRE_JAVA_IMAGE_TAG} -L

  ${rbe-configs-gen}/bin/rbe_configs_gen \
    --toolchain_container=rbe-autogen-lre-java:''${LRE_JAVA_IMAGE_TAG} \
    --exec_os=linux \
    --target_os=linux \
    --bazel_version=${bazel_7.version} \
    --output_src_root=''${SRC_ROOT} \
    --output_config_path=generated-java \
    --generate_java_configs=true \
    --generate_cpp_configs=false \
    --bazel_path=${bazel_7}/bin/bazel \
    --cpp_env_json=cpp_env.json

  # See comment above for C++.
  sed -i \
    's|rbe-autogen-lre-java|lre-java|g' \
    ''${SRC_ROOT}/generated-java/config/BUILD

  chmod 644 \
    ''${SRC_ROOT}/generated-java/LICENSE \
    ''${SRC_ROOT}/generated-java/config/BUILD \
    ''${SRC_ROOT}/generated-java/java/BUILD

  pre-commit run -a
''
