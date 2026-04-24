# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rustfmt_test")

# Invoke:
#   bazel test //tests/benchmarks/debug:bench_debug --compilation_mode=opt --test_output=streamed --cache_test_results=false --test_arg="--nocapture"  --test_arg="--bench"
rust_test(
    name = "bench_debug",
    srcs = ["bench_debug.rs"],
    deps = [
        "//answer",
        "//common/lending_iterator",
        "//common/options",
        "//database",
        "//executor",
        "//function",
        "//query",
        "//storage",

        "//util/test:test_utils",

        "@typeql//rust:typeql",

        "@crates//:criterion",
        "@crates//:pprof",
    ],
    data = [":schema.tql", ":data.tql", ":query.tql"],
    use_libtest_harness = False,
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":bench_debug",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
