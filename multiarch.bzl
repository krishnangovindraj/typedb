## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
#load("@vaticle_bazel_distribution//platform:constraints.bzl", "constraint_linux_arm64", "constraint_linux_x86_64",
#     "constraint_mac_arm64", "constraint_mac_x86_64", "constraint_win_x86_64")
#def _linux_x86_64_transition(settings, attr):
#    _ignore = (settings, attr)
#    return {"//command_line_option:platforms": "//:linux-x86_64"}
#
#def _linux_arm64_transition(settings, attr):
#    _ignore = (settings, attr)
#    return {"//command_line_option:platforms": "//:linux-arm64"}
#
#linux_x86_64_transition = transition(
#    implementation = _linux_x86_64_transition,
#    inputs = [],
#    outputs = ["//command_line_option:platforms"],
#)
#linux_arm64_transition = transition(
#    implementation = _linux_arm64_transition,
#    inputs = [],
#    outputs = ["//command_line_option:platforms"],
#)
#
#
#def _build_as_x86_64(ctx):
#    return DefaultInfo(files = depset(ctx.files.target))
#
#def _build_as_arm64(ctx):
#    return DefaultInfo(files = depset(ctx.files.target))
#
#build_as_x86_64 = rule(
#    implementation = _build_as_x86_64,
#    attrs = {
#        "target": attr.label(cfg = linux_x86_64_transition),
#        "_allowlist_function_transition": attr.label(
#            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
#        ),
#    },
#)
#
#build_as_arm64 = rule(
#    implementation = _build_as_arm64,
#    attrs = {
#        "target": attr.label(cfg = linux_x86_64_transition),
#        "_allowlist_function_transition": attr.label(
#            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
#        ),
#    },
#)
