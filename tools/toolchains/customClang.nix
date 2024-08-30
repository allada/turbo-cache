{
  stdenv,
  writeShellScriptBin,
}:
# Bazel expects a single frontend for both C and C++. That works for GCC but
# not for clang. This wrapper selects `clang` or `clang++` depending on file
# ending.
# TODO(aaronmondal): The necessity of this is a bug.
#                    See: https://github.com/NixOS/nixpkgs/issues/216047
#                    and https://github.com/NixOS/nixpkgs/issues/150655
writeShellScriptBin "customClang" ''
  #! ${stdenv.shell}
  function isCxx() {
     if [ $# -eq 0 ]; then false
     elif [ "$1" == '-xc++' ]; then true
     elif [[ -f "$1" && "$1" =~ [.](hh|H|hp|hxx|hpp|HPP|h[+]{2}|tcc|cc|cp|cxx|cpp|CPP|c[+]{2}|C)$ ]]; then true
     else isCxx "''${@:2}"; fi
  }
  if isCxx "$@"; then
    exec "${stdenv.cc}/bin/clang++" "$@"
  else
    exec "${stdenv.cc}/bin/clang" "$@"
  fi
''
