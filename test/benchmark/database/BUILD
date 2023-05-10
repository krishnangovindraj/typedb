#
# Copyright (C) 2022 Vaticle
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")

native_java_libraries(
    name = "database-benchmark-rocksdb-lib",
    srcs = [
        "DatabaseBenchmark.java",
        "DataSource.java",
        "Runner.java",
        "TypeDBOnRocks.java"
    ],
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//concept:concept",
        "//common:common",
        "//concurrent:concurrent",
    ],
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External Maven dependencies
        "@maven//:org_slf4j_slf4j_api",
    ],
)

native_java_libraries(
    name = "database-benchmark-speedb-lib",
    srcs = [
        "DatabaseBenchmark.java",
        "DataSource.java",
        "Runner.java",
        "TypeDBOnRocks.java"
    ],
    native_libraries_deps = [
        "//:typedb",
        "//database:database-speedb",
        "//concept:concept",
        "//common:common",
        "//concurrent:concurrent",
    ],
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External Maven dependencies
        "@maven//:org_slf4j_slf4j_api",
    ],
)

java_binary(
    name = "benchmark-rocksdb-mac",
    main_class = "com.vaticle.typedb.core.benchmark.database.Runner",
    runtime_deps = ["database-benchmark-rocksdb-lib-mac"]
)

java_binary(
    name = "benchmark-rocksdb-linux",
    main_class = "com.vaticle.typedb.core.benchmark.database.Runner",
    runtime_deps = ["database-benchmark-rocksdb-lib-linux"]
)

java_binary(
    name = "benchmark-rocksdb-windows",
    main_class = "com.vaticle.typedb.core.benchmark.database.Runner",
    runtime_deps = ["database-benchmark-rocksdb-lib-windows"]
)

java_binary(
    name = "benchmark-speedb-mac",
    main_class = "com.vaticle.typedb.core.benchmark.database.Runner",
    runtime_deps = ["database-benchmark-speedb-lib-mac"]
)

java_binary(
    name = "benchmark-speedb-linux",
    main_class = "com.vaticle.typedb.core.benchmark.database.Runner",
    runtime_deps = ["database-benchmark-speedb-lib-linux"]
)

java_binary(
    name = "benchmark-speedb-windows",
    main_class = "com.vaticle.typedb.core.benchmark.database.Runner",
    runtime_deps = ["database-benchmark-speedb-lib-windows"]
)
