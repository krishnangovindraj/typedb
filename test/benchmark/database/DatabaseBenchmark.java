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
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;

public class DatabaseBenchmark {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseBenchmark.class);
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("database-benchmark");

    private final DataSource dataSource;
    private final int vertexBatchSize;
    private final int edgeBatchSize;
    private final int queryBatchSize;
    private final TestSubject subject;

    public DatabaseBenchmark(int nVertices, int nEdges, int nQueries, int seed,
                             int vertexBatchSize, int edgeBatchSize, int queryBatchSize,
                             DatabaseBenchmark.TestSubject subject) {
        this.dataSource = new DataSource(nVertices, nEdges, nQueries, seed);
        this.vertexBatchSize = vertexBatchSize;
        this.edgeBatchSize = edgeBatchSize;
        this.queryBatchSize = queryBatchSize;
        this.subject = subject;
    }

    public long[] insertVertices() {
        FunctionalIterator<Long> vertices = dataSource.vertices();
        int batch = 0;
        long[] times = new long[dataSource.nVertices/vertexBatchSize];
        long start = System.nanoTime();
        while (vertices.hasNext()) {
            subject.openWriteTransaction();
            for (int i = 0; i < vertexBatchSize && vertices.hasNext(); i++) {
                subject.insertVertex(vertices.next());
            }
            subject.commitWrites();
            long lap = System.nanoTime();
            times[batch++] = (lap - start) / 1000;
            start = lap;
        }

        return times;
    }


    public long[] insertEdges() {
        FunctionalIterator<Pair<Long, Long>> edges = dataSource.edges();
        int batch = 0;
        long[] times = new long[dataSource.nEdges/edgeBatchSize];
        long start = System.nanoTime();
        while (edges.hasNext()) {
            subject.openWriteTransaction();
            for (int i = 0; i < edgeBatchSize && edges.hasNext(); i++) {
                Pair<Long, Long> edge = edges.next();
                subject.insertEdge(edge.first(), edge.second());
            }
            subject.commitWrites();
            long lap = System.nanoTime();
            times[batch++] = (lap - start) / 1000;
            start = lap;
        }

        return times;
    }

    public long[] runQueries() {
        FunctionalIterator<Long> queries = dataSource.queries();
        int batch = 0;
        long[] times = new long[dataSource.nQueries/queryBatchSize];
        long start = System.nanoTime();
        while (queries.hasNext()) {
            subject.openReadTransaction();
            for (int i=0; i< queryBatchSize && queries.hasNext(); i++) {
                subject.queryEdges(queries.next()).toList();
            }
            subject.closeRead();
            long lap = System.nanoTime();
            times[batch++] = (lap - start) / 1000;
            start = lap;
        }

        return times;
    }


    private void setup() {
        try {
            if (Files.exists(dataDir)) {
                System.out.println("Database directory exists!");
                Files.walk(dataDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
                System.out.println("Database directory deleted!");
            }

            Files.createDirectory(dataDir);

            System.out.println("Database directory created: " + dataDir);

        } catch (IOException e) {
            throw TypeDBException.of(new RuntimeException("Setup failed"));
        }
    }

    public void run() {
        this.setup();
        LOG.info("Setting up...");
        subject.setup(dataDir, subject.getClass().getSimpleName());
        LOG.info("insertVertices...");
        long[] verticesMicros = insertVertices();
        LOG.info("insertEdges...");
        long[] edgesMicros = insertEdges();
        LOG.info("runQueries...");
        long[] queriesMicros = runQueries();
        LOG.info("Done!");
        System.out.println();
        System.out.println(String.format("Batch timings (micros)\n* Vertex: %s\n* Edge: %s\n* Queries: %s",
                Arrays.toString(verticesMicros), Arrays.toString(edgesMicros), Arrays.toString(queriesMicros)));
    }

    public interface TestSubject {

        void setup(Path dataDir, String database);

        void tearDown();

        void openWriteTransaction();

        void commitWrites();

        void insertVertex(long id);

        void insertEdge(long from, long to);

        FunctionalIterator<Long> queryEdges(long from);

        void openReadTransaction();

        void closeRead();
    }
}
