exports_files([".rustfmt.toml"])

genrule(
    name = "dummy_test_sh",
    outs = ["dummy_test.sh"],
    cmd = "echo \"sleep .1;   echo $$(printf '=%.0s' {1..100})\" > \"$@\"",
)

sh_test(
    name = "dummy_test",
    srcs = [":dummy_test_sh"],
)
