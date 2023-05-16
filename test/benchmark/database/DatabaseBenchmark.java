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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class DatabaseBenchmark {
    static final Logger LOG = LoggerFactory.getLogger(DatabaseBenchmark.class);
    static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("database-benchmark");

    final DataSource dataSource;
    final TestSubject subject;
    private final ExecutorService executorService;

    public DatabaseBenchmark(int nVertices, int nEdges, int nQueries, int seed,
                             int vertexBatchSize, int edgeBatchSize, int queryBatchSize,
                             int parallelisation, TestSubject subject) {
        assert nVertices > 0 && nEdges > 0 && nQueries > 0;
        assert vertexBatchSize > 0 && edgeBatchSize > 0 && queryBatchSize > 0;
        this.dataSource = new DataSource(nVertices, nEdges, nQueries, vertexBatchSize, edgeBatchSize, queryBatchSize, seed);
        this.subject = subject;
        this.executorService = Executors.newFixedThreadPool(parallelisation);
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

    public Summary run() {
        this.setup();
        LOG.debug("Setting up...");
        subject.setup(subject.getClass().getSimpleName());

        LOG.debug("insertVertices...");
        long vertexStart = System.nanoTime();
        List<Long> vertexBatchTimes = insertVertices();
        long vertexTotalTime = (System.nanoTime() - vertexStart)/1000;

        LOG.debug("insertEdges...");
        long edgeStart = System.nanoTime();
        List<Long> edgeBatchTimes = insertEdges();
        long edgeTotalTime = (System.nanoTime() - edgeStart)/1000;

        LOG.debug("runQueries...");
        long queryStart = System.nanoTime();
        List<Long> queryBatchTimes = runQueries();
        long queryTotalTime = (System.nanoTime() - queryStart)/1000;

        LOG.debug("Done!");
        subject.tearDown();

        LOG.debug(String.format("Total/Batch timings (micros)\n* Vertex: %s %s\n* Edge: %s %s\n* Queries: %s %s",
                vertexTotalTime, vertexBatchTimes.toString(),
                edgeTotalTime, edgeBatchTimes.toString(),
                queryTotalTime, queryBatchTimes.toString()));

        return new Summary(this, vertexTotalTime, edgeTotalTime, queryTotalTime, vertexBatchTimes, edgeBatchTimes, queryBatchTimes);
    }

    public List<Long> insertVertices() {
        return processBatches(dataSource.vertexBatches(), this::processVertexBatch);
    }

    public List<Long> insertEdges() {
        return processBatches(dataSource.edgeBatches(), this::processEdgeBatch);
    }

    public List<Long> runQueries() {
        return processBatches(dataSource.queryBatches(), this::processQueryBatch);
    }

    private void processVertexBatch(FunctionalIterator<Long> batch) {
        try (TestSubject.Transaction tx = subject.writeTransaction()) {
            batch.forEachRemaining(vertex -> subject.insertVertex(tx, vertex));
            tx.commit();
        } catch (Exception e) {
            throw TypeDBException.of(e);
        }
    }

    private void processEdgeBatch(FunctionalIterator<Pair<Long,Long>> batch) {
        try (TestSubject.Transaction tx = subject.writeTransaction()) {
            batch.forEachRemaining(edge -> subject.insertEdge(tx, edge.first(), edge.second()));
            tx.commit();
        } catch (Exception e) {
            throw TypeDBException.of(e);
        }
    }

    private void processQueryBatch(FunctionalIterator<Long> batch) {
        try (TestSubject.Transaction tx = subject.writeTransaction()) {
            batch.forEachRemaining(vertex -> subject.queryEdges(tx, vertex).count());
        } catch (Exception e) {
            throw TypeDBException.of(e);
        }
    }

    public <T> List<Long> processBatches(List<DataSource.BatchGenerator<T>> batches, Consumer<FunctionalIterator<T>> batchProcessor) {
        List<Future<Long>> timeFutures = new ArrayList<>();
        batches.forEach(batch -> {
            timeFutures.add(executorService.submit(() -> {
                long start = System.nanoTime();
                batchProcessor.accept(batch.generate());
                return (System.nanoTime() - start)/1000;
            }));
        });

        return iterate(timeFutures).map(future -> {
            try {
                return future.get();
            } catch (InterruptedException|ExecutionException e) {
                throw TypeDBException.of(e);
            }
        }).toList();
    }

    public interface TestSubject<TRANSACTION extends TestSubject.Transaction> {

        void setup(String database);

        void tearDown();

        TRANSACTION writeTransaction();

        TRANSACTION readTransaction();

        void insertVertex(TRANSACTION tx, long id);


        void insertEdge(TRANSACTION tx, long from, long to);

        FunctionalIterator<Long> queryEdges(TRANSACTION tx, long from);

        interface Transaction extends AutoCloseable {
            void commit();
        }
    }

    public static class Summary {

        final DatabaseBenchmark benchmark;
        final long vertexTotalTime;
        final long edgeTotalTime;
        final long queryTotalTime;
        final List<Long> vertexBatchTimes;
        final List<Long> edgeBatchTimes;
        final List<Long> queryBatchTimes;

        private Summary(DatabaseBenchmark benchmark,
                        long vertexTotalTime, long edgeTotalTime, long queryTotalTime,
                        List<Long> vertexBatchTimes, List<Long> edgeBatchTimes, List<Long> queryBatchTimes) {
            this.benchmark = benchmark;
            this.vertexTotalTime = vertexTotalTime;
            this.edgeTotalTime = edgeTotalTime;
            this.queryTotalTime = queryTotalTime;
            this.vertexBatchTimes = vertexBatchTimes;
            this.edgeBatchTimes = edgeBatchTimes;
            this.queryBatchTimes = queryBatchTimes;
        }
    }
}
