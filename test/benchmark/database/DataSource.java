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
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.AbstractFunctionalIterator;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public class DataSource {

    private final int seed;
    final int nVertices;
    final int nEdges;
    final int nQueries;

    DataSource(int nVertices, int nEdges, int nQueries, int seed) {
        this.nVertices = nVertices;
        this.nEdges = nEdges;
        this.nQueries = nQueries;
        this.seed = seed;
    }

    FunctionalIterator<Long> vertices() {
        List<Long> shuffled = new ArrayList<>();
        for (int i = 0; i < nVertices; i++) {
            shuffled.add(Long.valueOf(i));
        }
        Collections.shuffle(shuffled);
        return Iterators.iterate(shuffled);
    }

    FunctionalIterator<Pair<Long, Long>> edges() {
        return new EdgeGenerator();
    }

    public FunctionalIterator<Long> queries() {
        return new QueryGenerator();
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

    private class QueryGenerator extends AbstractFunctionalIterator<Long> {

        private int nGenerated;
        private final Random random;

        private QueryGenerator() {
            this.nGenerated = 0;
            this.random = new Random(seed);
        }

        @Override
        public void recycle() {
            throw TypeDBException.of(UNIMPLEMENTED);
        }

        @Override
        public boolean hasNext() {
            return nGenerated < nQueries;
        }

        @Override
        public Long next() {
            nGenerated++;
            return Long.valueOf(random.nextInt(nVertices));
        }
    }

}
