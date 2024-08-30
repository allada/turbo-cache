{
  nativelink,
  writeShellScriptBin,
}:
writeShellScriptBin "is-executable-test" ''
  set -xuo pipefail

  nativelink_output="$(${nativelink}/bin/nativelink 2>&1)"

  print_error_output=$(cat <<EOF
  error: the following required arguments were not provided:
    <CONFIG_FILE>

  Usage: nativelink <CONFIG_FILE>

  For more information, try '--help'.
  EOF)

  if [ "$nativelink_output" = "$print_error_output" ]; then
    echo "The output of nativelink matches the print_error output."
  else
    echo 'The output of nativelink does not match the print_error output:'
    diff <(echo "$nativelink_output") <(echo "$print_error_output") >&2
    exit 1
  fi
''
