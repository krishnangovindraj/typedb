package com.vaticle.typedb.core.benchmark.database;

import org.junit.Test;

public class Runner {

    private static final int NVERTEX = 100;
    private static final int NEDGE = 500;
    private static final int SEED = 12345;
    private static final int VERTEXBATCH = 10;
    private static final int EDGEBATCH = 10;

    @Test
    public void testTypeDBOnRocks() {
        new DatabaseBenchmark(NVERTEX, NEDGE, SEED,
                VERTEXBATCH, EDGEBATCH,
                new TypeDBOnRocks()).run();
    }

    public static void main(String[] args) {
        new Runner().testTypeDBOnRocks();
    }
}
