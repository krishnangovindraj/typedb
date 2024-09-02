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

import static com.vaticle.typedb.core.common.collection.Bytes.MB;

public class Benchmark {


    private static final String database = "basic-test";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageIndexCacheSize(MB).storageDataCacheSize(MB);
    public static void main(String[] args) throws IOException {
        Util.resetDirectory(dataDir);
        try (TypeDB.DatabaseManager typedb = CoreDatabaseManager.open(options)) {
            typedb.create(database);
            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
                try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    transaction.query().define(TypeQL.parseQuery("define person sub entity, owns age; age sub attribute, value long;"));
                    transaction.commit();
                }
            }
            long iters = 500_000;

            Random random = new Random();
            long start = System.nanoTime();
            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.DATA)) {
                for (int i = 0; i < iters; i++) {
                    try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                        long age = random.nextLong();
                        transaction.query().insert(TypeQL.parseQuery("insert $x isa person, has age " + age + ";").asInsert());
                        transaction.commit();
                    }
                }
            }
            long elapsed = System.nanoTime() - start;
            System.out.printf("Total time for %d transactions + iterations: %d (%d nanos per insert) \n", iters, elapsed, elapsed / iters);
        }
    }
}