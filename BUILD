# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//tool/release/deps:rules.bzl", "release_validate_deps")

load("@vaticle_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
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
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_mac_arm64,
)

assemble_targz(
    name = "assemble-linux-x86_64-targz",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-linux-x86_64",
    permissions = permissions,
    targets = ["//:package-typedb"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_linux_x86_64,
)

deploy_artifact(
    name = "deploy-mac-arm64-zip",
    artifact_group = "typedb-all-mac-arm64",
    artifact_name = "typedb-all-mac-arm64-{version}.zip",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-mac-arm64-zip",
)

deploy_artifact(
    name = "deploy-linux-x86_64-targz",
    artifact_group = "typedb-all-linux-x86_64",
    artifact_name = "typedb-all-linux-x86_64-{version}.tar.gz",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-linux-x86_64-targz",
)

# Convenience
alias(
    name = "assemble-typedb-server",
    actual = select({
#        "@vaticle_bazel_distribution//platform:is_linux_arm64" : ":assemble-linux-arm64-targz",
        "@vaticle_bazel_distribution//platform:is_linux_x86_64" : ":assemble-linux-x86_64-targz",
        "@vaticle_bazel_distribution//platform:is_mac_arm64" : ":assemble-mac-arm64-zip",
#        "@vaticle_bazel_distribution//platform:is_mac_x86_64" : ":assemble-mac-x86_64-zip",
#        "@vaticle_bazel_distribution//platform:is_windows_x86_64" : ":assemble-windows-x86_64-zip"
    }),
    visibility = ["//tests/assembly:__subpackages__"],
)
alias(
    name = "deploy-typedb-server",
    actual = select({
#        "@vaticle_bazel_distribution//platform:is_linux_arm64" : ":deploy-linux-arm64-targz",
        "@vaticle_bazel_distribution//platform:is_linux_x86_64" : ":deploy-linux-x86_64-targz",
        "@vaticle_bazel_distribution//platform:is_mac_arm64" : ":deploy-mac-arm64-zip",
#        "@vaticle_bazel_distribution//platform:is_mac_x86_64" : ":deploy-mac-x86_64-zip",
#        "@vaticle_bazel_distribution//platform:is_windows_x86_64" : ":deploy-windows-x86_64-zip"
    })
)
