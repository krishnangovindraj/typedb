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

public class Runner {

    private static final int SEED = 12345;
    private static final int STEPS = 1;

    // Initial values
    private static final int INIT_VERTEX = 10000;
    private static final int INIT_AVGDEGREE = 5;
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
        for (int i = 0; i < STEPS; i++) {
            new DatabaseBenchmark(nVertices, nVertices * avgDegree, nQueries, SEED,
            vertexBatchSize, edgeBatchSize, queryBatchSize,
            new TypeDBOnRocks()).run();
            step();
        }
    }

    public static void main(String[] args) {
        new Runner().run();
    }
}
