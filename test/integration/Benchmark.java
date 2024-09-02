package com.vaticle.typedb.core.test.integration;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;

public class Benchmark {


    private static final String database = "basic-test";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageIndexCacheSize(MB).storageDataCacheSize(MB);
    public static void main(String[] args) throws IOException, InterruptedException {
        Util.resetDirectory(dataDir);
        try (TypeDB.DatabaseManager typedb = CoreDatabaseManager.open(options)) {
            typedb.create(database);
            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
                try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    transaction.query().define(TypeQL.parseQuery("define person sub entity, owns age; age sub attribute, value long;"));
                    transaction.commit();
                }
            }
            int N_THREADS = 4;
            int N_ITERS = 100_000;

            Random random = new Random();
            Thread[] threads = new Thread[N_THREADS];
            CountDownLatch signal = new CountDownLatch(1);

            for (int i = 0; i < N_THREADS; i++) {
                int thread_id = i;
                threads[thread_id] = new Thread(() -> {
                    try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.DATA)) {
                        try {
                            signal.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        for (int i1 = 0; i1 < N_ITERS; i1++) {
                            try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                                long age = random.nextLong();
                                transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has age " + age + ";").asInsert());
                                transaction.commit();
                            }
                        }
                    }
                });
            }

            for (int i = 0; i< N_THREADS; i++) {
                threads[i].start();
            }
            long start = System.nanoTime();
            signal.countDown();
            long start_took = System.nanoTime() - start;
            for (int i=0 ; i < N_THREADS; i++) {
                threads[i].join();
            }
            double elapsed_micros = (System.nanoTime() - start)/ 1000.0;
            System.out.printf("Total time for %d transactions + iterations: %.2f us (%.2f inserts/s) \n", N_ITERS, elapsed_micros, (1000_000.0 * N_ITERS * N_THREADS) / ( elapsed_micros));
            System.out.printf("Start took: %d nanos", start_took);
        }
    }
}