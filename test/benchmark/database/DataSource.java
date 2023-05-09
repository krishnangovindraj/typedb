package com.vaticle.typedb.core.benchmark.database;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.AbstractFunctionalIterator;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.Random;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public class DataSource {

    private final int seed;
    private final int nVertices;
    private final int nEdges;

    DataSource(int nVertices, int nEdges, int seed) {
        this.nVertices = nVertices;
        this.nEdges = nEdges;
        this.seed = seed;
    }

    FunctionalIterator<Long> vertices() {
        return new VertexGenerator();
    }

    FunctionalIterator<Pair<Long, Long>> edges() {
        return new EdgeGenerator();
    }

    private class VertexGenerator extends AbstractFunctionalIterator<Long> {

        private long nextId;

        public VertexGenerator() {
            nextId = 0;
        }

        @Override
        public void recycle() {
            throw TypeDBException.of(UNIMPLEMENTED);
        }

        @Override
        public boolean hasNext() {
            return nextId < nVertices;
        }

        @Override
        public Long next() {
            return nextId++;
        }
    }

    private class EdgeGenerator extends AbstractFunctionalIterator<Pair<Long, Long>> {

        private int nGenerated;
        private final Random random;

        private EdgeGenerator() {
            this.nGenerated = 0;
            this.random = new Random(seed);
        }

        @Override
        public void recycle() {
            throw TypeDBException.of(UNIMPLEMENTED);
        }

        @Override
        public boolean hasNext() {
            return nGenerated < nEdges;
        }

        @Override
        public Pair<Long, Long> next() {
            long first = random.nextInt(nVertices);
            long second;
            do {
                second = random.nextInt(nVertices);
            } while (second == first);
            nGenerated++;
            return new Pair<>(first, second);
        }
    }

}
