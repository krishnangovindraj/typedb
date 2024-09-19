#
#load("@io_bazel_rules_docker//container:import.bzl", "container_import")
#load("@vaticle_bazel_distribution//platform:constraints.bzl", "constraint_linux_arm64", "constraint_linux_x86_64",
#     "constraint_mac_arm64", "constraint_mac_x86_64", "constraint_win_x86_64")
#
#platform(
#    name = "linux-x86_64",
#    constraint_values = constraint_linux_x86_64,
#)
#platform(
#    name = "linux-arm64",
#    constraint_values = constraint_linux_arm64,
#)
#
#container_import(
#    name = "ubuntu-22.04-x86_64",
#    config = ":amd64.json",
#    layers = [],
#    base_image_registry = "index.docker.io",
#    base_image_repository = "ubuntu",
#    base_image_digest = "sha256:3d1556a8a18cf5307b121e0a98e93f1ddf1f3f8e092f1fddfd941254785b95d7",
##    tags = ["jammy"],
#    architecture = "amd64",
#)
#
#container_import(
#    name = "ubuntu-22.04-arm64",
#    config = ":arm64.json",
#    layers = [],
#    base_image_registry = "index.docker.io",
#    base_image_repository = "ubuntu",
#    base_image_digest = "sha256:7c75ab2b0567edbb9d4834a2c51e462ebd709740d1f2c40bcd23c56e974fe2a8",
##    tags = ["jammy"],
#    architecture = "arm64v8",
#)