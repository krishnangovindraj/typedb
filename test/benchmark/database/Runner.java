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
