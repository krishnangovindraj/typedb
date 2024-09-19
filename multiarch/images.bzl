
package(default_visibility = ["//visibility:public"])
load("@io_bazel_rules_docker//container:import.bzl", "container_import")


def base_images():
    container_import(
        name = "ubuntu-22.04-x86_64",
        config = "amd64.json",
        layers = [],
        base_image_registry = "index.docker.io",
        base_image_repository = "amd64/ubuntu",
        base_image_digest = "sha256:3d1556a8a18cf5307b121e0a98e93f1ddf1f3f8e092f1fddfd941254785b95d7",
        tags = [""],
    )

    container_import(
        name = "ubuntu-22.04-arm64",
        config = "arm64.json",
        layers = [],
        base_image_registry = "index.docker.io",
        base_image_repository = "arm64v8/ubuntu",
        base_image_digest = "sha256:7c75ab2b0567edbb9d4834a2c51e462ebd709740d1f2c40bcd23c56e974fe2a8",
        tags = [""],
    )
    #
    #exports_files(["image.digest", "digest"])
