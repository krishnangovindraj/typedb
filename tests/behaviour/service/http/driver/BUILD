# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rustfmt_test")

rust_test(
    name = "test_http_driver",
    crate_root = "main.rs",
    srcs = ["main.rs", "concept.rs", "connection.rs", "http.rs", "query.rs", "user.rs"],
    deps = [
        "//tests/behaviour/service/http/http_steps:http_steps",

        "@crates//:tokio",
    ],
    data = [
        "@typedb_behaviour//driver:concept.feature",
        "@typedb_behaviour//driver:connection.feature",
        "@typedb_behaviour//driver/http:http.feature",
        "@typedb_behaviour//driver:query.feature",
        "@typedb_behaviour//driver:user.feature",
    ],
    crate_features = ["bazel"],
    size = "enormous",
    tags = ["manual"],
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":test_http_driver",
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
