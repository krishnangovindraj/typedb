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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class DataSource {
    final int nVertices;
    final int nEdges;
    final int nQueries;

    final int vertexBatchSize;
    final int edgeBatchSize;
    final int queryBatchSize;
    private final Random metaRandom;

    DataSource(int nVertices, int nEdges, int nQueries, int vertexBatchSize, int edgeBatchSize, int queryBatchSize, int seed) {
        this.nVertices = nVertices;
        this.nEdges = nEdges;
        this.nQueries = nQueries;
        this.vertexBatchSize = vertexBatchSize;
        this.edgeBatchSize = edgeBatchSize;
        this.queryBatchSize = queryBatchSize;
        this.metaRandom = new Random(seed);
    }

    public List<BatchGenerator<Long>> vertexBatches() {
        return batchGenerator(nVertices, vertexBatchSize, this::createVertexBatch);
    }

    public List<BatchGenerator<Pair<Long,Long>>> edgeBatches() {
        return batchGenerator(nEdges, edgeBatchSize, this::createEdgeBatch);
    }


    public List<BatchGenerator<Long>> queryBatches() {
        return batchGenerator(nQueries, queryBatchSize, this::createQueryBatch);
    }


    private FunctionalIterator<Long> createVertexBatch(BatchGenerator<Long> batchGenerator) {
        ArrayList<Long> vertices = new ArrayList<>(batchGenerator.batchSize);
        Random random = new Random(batchGenerator.seed);
        for (int i = 0; i < batchGenerator.batchSize; i++) {
            vertices.add((long) i * batchGenerator.nBatches + batchGenerator.batchIdx);
        }
        Collections.shuffle(vertices, random);
        return iterate(vertices);
    }

    private FunctionalIterator<Pair<Long, Long>> createEdgeBatch(BatchGenerator<Pair<Long,Long>> batchGenerator) {
        Random random = new Random(batchGenerator.seed);
        return repeat(batchGenerator.batchSize)
                .map(i -> new Pair<>(nextLong(random, nVertices), nextLong(random, nVertices)));
    }

    private FunctionalIterator<Long> createQueryBatch(BatchGenerator<Long> batchGenerator) {
        Random random = new Random(batchGenerator.seed);
        return repeat(batchGenerator.batchSize).map(i -> nextLong(random, nVertices));
    }


    private static long nextLong(Random random, int bound) {
        return random.nextInt(bound); // TODO
    }

    private static FunctionalIterator<Integer> repeat(int n) {
        final Integer N = n;
        return Iterators.loop(n, i -> i > 0, i -> i - 1).map(i -> N-i);
    }

    private <T> List<BatchGenerator<T>> batchGenerator(int total, int batchSize, Function<BatchGenerator<T>, FunctionalIterator<T>> generator) {
        int nBatches = total / batchSize + ((total % batchSize == 0) ? 0 : 1);
        List<BatchGenerator<T>> batches = new ArrayList<>(nBatches);
        int idx = 0;
        int remaining = total;
        while (remaining > 0) {
            int nextBatchSize = Math.min(remaining, batchSize);
            remaining -= nextBatchSize;
            batches.add(new BatchGenerator<>(idx++, nBatches, nextBatchSize, metaRandom.nextInt(), generator));
        }
        return batches;
    }

    public static class BatchGenerator<T> {
        private final int batchIdx;
        private final int nBatches;
        private final int batchSize;
        private final int seed;
        private final Function<BatchGenerator<T>, FunctionalIterator<T>> generator;

        BatchGenerator(int batchIdx, int nBatches, int batchSize, int seed, Function<BatchGenerator<T>, FunctionalIterator<T>> generator) {
            this.batchIdx = batchIdx;
            this.nBatches = nBatches;
            this.batchSize = batchSize;
            this.seed = seed;
            this.generator = generator;
        }

        public FunctionalIterator<T> generate() {
            return generator.apply(this);
        }
    }
}
