package com.vaticle.typedb.core.benchmark.database;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class DatabaseBenchmark {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("database-benchmark");

    private final DataSource dataSource;
    private final int vertexBatchSize;
    private final int edgeBatchSize;
    private final TestSubject subject;

    public DatabaseBenchmark(int nVertices, int nEdges, int seed,
                             int vertexBatchSize, int edgeBatchSize,
                             DatabaseBenchmark.TestSubject subject) {
        this.dataSource = new DataSource(nVertices, nEdges, seed);
        this.vertexBatchSize = vertexBatchSize;
        this.edgeBatchSize = edgeBatchSize;
        this.subject = subject;
    }

    public long insertVertices() {
        long start = System.nanoTime();

        FunctionalIterator<Long> vertices = dataSource.vertices();
        while (vertices.hasNext()) {
            subject.openTransaction();
            for (int i = 0; i < vertexBatchSize && vertices.hasNext(); i++) {
                subject.insertVertex(vertices.next());
            }
            subject.commit();
        }

        return (System.nanoTime() - start)/ 1000;
    }


    public long insertEdges() {
        long start = System.nanoTime();

        FunctionalIterator<Pair<Long, Long>> edges = dataSource.edges();
        while (edges.hasNext()) {
            subject.openTransaction();
            for (int i = 0; i < edgeBatchSize && edges.hasNext(); i++) {
                Pair<Long, Long> edge = edges.next();
                subject.insertEdge(edge.first(), edge.second());
            }
            subject.commit();
        }

        return (System.nanoTime() - start)/ 1000;
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
        subject.setup(dataDir, subject.getClass().getSimpleName());

        long verticesMicros = insertVertices();

        long edgesMicros = insertEdges();

        System.out.println("Vertex/Edge (micros): "  + verticesMicros + " | " + edgesMicros);
    }

    public interface TestSubject {

        void setup(Path dataDir, String database);
        void tearDown();

        void openTransaction();
        void commit();

        void insertVertex(long id);
        void insertEdge(long from, long to);
        FunctionalIterator<Long> queryEdges(long from);
    }
}
