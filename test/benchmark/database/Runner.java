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

import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.common.parameters.Order;

import java.util.ArrayList;
import java.util.List;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class Runner {

    private static final int SEED = 12345;
    private static final int STEPS = 5;

    // Initial values
    private static final int INIT_VERTEX = 10000;
    private static final int INIT_AVGDEGREE = 10;
    private static final int INIT_QUERY = 10000;

    private static final int INIT_VERTEXBATCH = 10000;
    private static final int INIT_EDGEBATCH = 10000;
    private static final int INIT_QUERYBATCH = 10000;
    private static final int PARALLELISATION = 8;

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
                    PARALLELISATION, new TypeDBOnRocks(getOptions()));
            results.add(benchmark.run());
            step();
        }
        System.out.println(summariesToCSV(results));
    }

    private static Options.Database getOptions() {
        return new Options.Database().dataDir(DatabaseBenchmark.dataDir)
                .storageDataCacheSize(500 * MB )
                .storageIndexCacheSize(500 * MB);
    }

    private static String summariesToCSV(List<DatabaseBenchmark.Summary> summaries) {
        StringBuilder sb = new StringBuilder();
        sb
                .append("nVertices,").append("nEdges,").append("nQueries,")
                .append("vertexBatch,").append("edgeBatch,").append("queryBatch,")
                .append("vertexTotalTime,").append("edgeTotalTime,").append("queryTotalTime,")
                .append("vertexBatchTimeMean,").append("edgeBatchTimeMean,").append("queryBatchTimeMean,")
                .append("vertexBatchTime10p,").append("edgeBatchTime10p,").append("queryBatchTime10p,")
                .append("vertexBatchTime90p,").append("edgeBatchTime90p,").append("queryBatchTime90p,")
                .append("vertexBatchTimesAll,").append("edgeBatchTimesAll,").append("queryBatchTimesAll")
        .append("\n");

        for (DatabaseBenchmark.Summary summary : summaries) {
            sb
                    .append(summary.benchmark.dataSource.nVertices).append(",")
                    .append(summary.benchmark.dataSource.nEdges).append(",")
                    .append(summary.benchmark.dataSource.nQueries).append(",")

                    .append(summary.benchmark.dataSource.vertexBatchSize).append(",")
                    .append(summary.benchmark.dataSource.edgeBatchSize).append(",")
                    .append(summary.benchmark.dataSource.queryBatchSize).append(",")

                    .append(summary.vertexTotalTime).append(",")
                    .append(summary.edgeTotalTime).append(",")
                    .append(summary.queryTotalTime).append(",")

                    .append(mean(summary.vertexBatchTimes)).append(",")
                    .append(mean(summary.edgeBatchTimes)).append(",")
                    .append(mean(summary.queryBatchTimes)).append(",")

                    .append(percentile(summary.vertexBatchTimes, 10)).append(",")
                    .append(percentile(summary.edgeBatchTimes, 10)).append(",")
                    .append(percentile(summary.queryBatchTimes, 10)).append(",")

                    .append(percentile(summary.vertexBatchTimes, 90)).append(",")
                    .append(percentile(summary.edgeBatchTimes, 90)).append(",")
                    .append(percentile(summary.queryBatchTimes, 90)).append(",")

                    .append(concatList(summary.vertexBatchTimes)).append(",")
                    .append(concatList(summary.edgeBatchTimes)).append(",")
                    .append(concatList(summary.queryBatchTimes))
                    .append("\n");
        }
        return sb.toString();
    }

    private static String concatList(List<Long> arr) {
        return String.join(" ", iterate(arr).map(l -> l.toString()).toList());
    }

    private static double mean(List<Long> values) {
        return iterate(values).reduce(0.0, Double::sum) / values.size();
    }


    private static long percentile(List<Long> values, int p) {
        int nextIndex = ((p == 100) ? (values.size()-1) :  (values.size() * p)/100);
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Long::compare);
        return sorted.get(nextIndex);
    }

    public static void main(String[] args) {
        new Runner().run();
    }
}
