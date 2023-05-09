package com.vaticle.typedb.core.benchmark.database;

import org.junit.Test;

public class Runner {

    private static final int NVERTEX = 50000;
    private static final int NEDGE = 250000;
    private static final int NQUERY = 10000;
    private static final int SEED = 12345;
    private static final int VERTEXBATCH = 5000;
    private static final int EDGEBATCH = 5000;
    private static final int QUERYBATCH = 1000;

    @Test
    public void testTypeDBOnRocks() {
        new DatabaseBenchmark(NVERTEX, NEDGE, NQUERY, SEED,
                VERTEXBATCH, EDGEBATCH, QUERYBATCH,
                new TypeDBOnRocks()).run();
    }

    public static void main(String[] args) {
        new Runner().testTypeDBOnRocks();
    }
}
