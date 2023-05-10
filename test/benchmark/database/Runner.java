/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.benchmark.database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Runner {

    private static final int SEED = 12345;
    private static final int STEPS = 1;

    // Initial values
    private static final int INIT_VERTEX = 5000;
    private static final int INIT_AVGDEGREE = 10;
    private static final int INIT_QUERY = 10000;

    private static final int INIT_VERTEXBATCH = 5000;
    private static final int INIT_EDGEBATCH = 5000;
    private static final int INIT_QUERYBATCH = 1000;

    private int nVertices;
    private int avgDegree;
    private int nQueries;
    private int vertexBatchSize;
    private int edgeBatchSize;
    private int queryBatchSize;

    public Runner() {
        this.nVertices = INIT_VERTEX;
        this.avgDegree = INIT_AVGDEGREE;
        this.nQueries = INIT_QUERY;
        this.vertexBatchSize = INIT_VERTEXBATCH;
        this.edgeBatchSize = INIT_EDGEBATCH;
        this.queryBatchSize = INIT_QUERYBATCH;
    }

    private void step() {
        // Update STEPS and perform any updates here
    }

    public void run() {
        List<DatabaseBenchmark.Summary> results = new ArrayList<>();
        for (int i = 0; i < STEPS; i++) {
            DatabaseBenchmark benchmark = new DatabaseBenchmark(nVertices, nVertices * avgDegree, nQueries, SEED,
                    vertexBatchSize, edgeBatchSize, queryBatchSize,
                    new TypeDBOnRocks());
            results.add(benchmark.run());
            step();
        }
        System.out.println(summariesToCSV(results));
    }

    private static String summariesToCSV(List<DatabaseBenchmark.Summary> summaries) {
        StringBuilder sb = new StringBuilder();
        sb
                .append("nVertices,").append("nEdges,").append("nQueries,")
                .append("vertexBatch,").append("edgeBatch,").append("queryBatch,")
                .append("vertexTimeMean,").append("edgeTimeMean,").append("queryTimeMean,")
                .append("vertexTime90p,").append("edgeTime90p,").append("queryTime90p,")
                .append("vertexTimesAll,").append("edgeTimesAll,").append("queryTimesAll")
        .append("\n");

        for (DatabaseBenchmark.Summary summary : summaries) {
            sb
                    .append(summary.benchmark.dataSource.nVertices).append(",")
                    .append(summary.benchmark.dataSource.nEdges).append(",")
                    .append(summary.benchmark.dataSource.nQueries).append(",")

                    .append(summary.benchmark.vertexBatchSize).append(",")
                    .append(summary.benchmark.edgeBatchSize).append(",")
                    .append(summary.benchmark.queryBatchSize).append(",")

                    .append(mean(summary.vertexMicros)).append(",")
                    .append(mean(summary.edgeMicros)).append(",")
                    .append(mean(summary.queryMicros)).append(",")

                    .append(percentile(summary.vertexMicros, 90)).append(",")
                    .append(percentile(summary.edgeMicros, 90)).append(",")
                    .append(percentile(summary.queryMicros, 90)).append(",")

                    .append(String.join(" ", concatArray(summary.vertexMicros))).append(",")
                    .append(String.join(" ", concatArray(summary.edgeMicros))).append(",")
                    .append(String.join(" ", concatArray(summary.queryMicros)))
                    .append("\n");
        }
        return sb.toString();
    }

    private static List<String> concatArray(long[] arr) {
        return Arrays.stream(arr).mapToObj(String::valueOf).collect(Collectors.toList());
    }

    private static double mean(long[] arr) {
        return ((double)Arrays.stream(arr).reduce(0, Long::sum)) / arr.length;
    }

    private static long percentile(long[] arr, int p) {
        int nextIndex = ((p == 100) ? (arr.length-1) :  (arr.length * p)/100);
        return arr[nextIndex];
    }

    public static void main(String[] args) {
        new Runner().run();
    }
}
