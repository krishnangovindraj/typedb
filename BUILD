# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//tool/release/deps:rules.bzl", "release_validate_deps")

load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_versioned", "assemble_zip")
load("@vaticle_bazel_distribution//platform:constraints.bzl", "constraint_linux_arm64", "constraint_linux_x86_64",
     "constraint_mac_arm64", "constraint_mac_x86_64", "constraint_win_x86_64")

load("@rules_pkg//:pkg.bzl", "pkg_tar")
load("@rules_rust//rust:defs.bzl", "rust_binary")

exports_files(
    ["VERSION", "deployment.bzl", "LICENSE", "README.md"],
)

rust_binary(
    name = "typedb_server_bin",
    srcs = [
        "main.rs",
    ],
    deps = [
        "//common/logger",
        "//database",
        "//resource",
        "//server",

        "@crates//:tokio",
    ],
)




checkstyle_test(
    name = "checkstyle",
    include = glob(["*", ".factory/*", "bin/*", ".circleci/*"]),
    exclude = glob([
        "*.md",
        ".circleci/windows/*",
        "docs/*",
        "tools/**",
        "Cargo.*",
    ]) + [
        ".bazelversion",
        ".bazel-remote-cache.rc",
        ".bazel-cache-credential.json",
        "LICENSE",
        "VERSION",
    ],
    license_type = "mpl-header",
)

checkstyle_test(
    name = "checkstyle_license",
    include = ["LICENSE"],
    license_type = "mpl-fulltext",
)

filegroup(
    name = "tools",
    data = [
        "@vaticle_dependencies//factory/analysis:dependency-analysis",
        "@vaticle_dependencies//tool/bazelinstall:remote_cache_setup.sh",
        "@vaticle_dependencies//tool/release/notes:create",
        "@vaticle_dependencies//tool/checkstyle:test-coverage",
        "@vaticle_dependencies//tool/unuseddeps:unused-deps",
        "@rust_analyzer_toolchain_tools//lib/rustlib/src:rustc_srcs",
        "@vaticle_dependencies//tool/ide:rust_sync",
    ],
)

# Assembly
assemble_files = {
    "//resource:logo": "resource/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}
empty_directories = [
    "server/data",
]
permissions = {
    "server/conf/config.yml": "0755",
    "server/data": "0755",
}

pkg_tar(
    name = "package-typedb",
    srcs = ["//:typedb_server_bin"],
)

assemble_zip(
    name = "assemble-mac-arm64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-mac-arm64",
    permissions = permissions,
    targets = ["//:package-typedb"],
    target_compatible_with = constraint_mac_arm64,
)

assemble_targz(
    name = "assemble-linux-x86_64-tgz",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-linux-x86_64",
    permissions = permissions,
    targets = ["//:package-typedb"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_linux_x86_64,
)

# TODO: assemble_versioned
