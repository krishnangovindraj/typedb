/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.behaviour.connection;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.diagnostics.Diagnostics;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.database.CoreDatabase;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConnectionSteps {

    public static int THREAD_POOL_SIZE = 32;
    public static ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public static CoreDatabaseManager databaseMgr;
    public static Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("typedb");
    public static Path logsDir = dataDir.resolve("logs");
    public static Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logsDir)
            .storageIndexCacheSize(MB).storageDataCacheSize(MB);
    public static List<TypeDB.Session> sessions = new ArrayList<>();
    public static List<CompletableFuture<TypeDB.Session>> sessionsParallel = new ArrayList<>();
    public static Map<TypeDB.Session, List<TypeDB.Transaction>> sessionsToTransactions = new HashMap<>();
    public static Map<TypeDB.Session, List<CompletableFuture<TypeDB.Transaction>>> sessionsToTransactionsParallel = new HashMap<>();
    public static Map<CompletableFuture<TypeDB.Session>, List<CompletableFuture<TypeDB.Transaction>>> sessionsParallelToTransactionsParallel = new HashMap<>();

    public static TypeDB.Transaction tx() {
        assertFalse("There is no open session", sessions.isEmpty());
        assertFalse("There is no open transaction", sessionsToTransactions.get(sessions.get(0)).isEmpty());
        return sessionsToTransactions.get(sessions.get(0)).get(0);
    }

    @After
    public synchronized void after() {
        if (databaseMgr.isOpen()) {
            System.out.println("ConnectionSteps.after");
            sessionsToTransactions.values().forEach(l -> l.forEach(TypeDB.Transaction::close));
            sessionsToTransactions.clear();
            sessionsToTransactionsParallel.values().forEach(l -> l.forEach(c -> {
                try {
                    c.get().close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
            sessionsToTransactionsParallel.clear();
            sessionsParallelToTransactionsParallel.values().forEach(l -> l.forEach(c -> {
                try {
                    c.get().close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
            sessionsParallelToTransactionsParallel.clear();
            sessions.forEach(TypeDB.Session::close);
            sessions.clear();
            sessionsParallel.forEach(c -> c.thenAccept(TypeDB.Session::close));
            sessionsParallel.clear();
            databaseMgr.all().forEach(CoreDatabase::delete);
            databaseMgr.close();
            assertFalse(databaseMgr.isOpen());
            databaseMgr = null;
        }
    }

    @Given("typedb starts")
    public void typedb_starts() throws IOException {
        assertNull(databaseMgr);
        resetDirectory();
        Diagnostics.Noop.initialise();
        System.out.println("Connecting to TypeDB ...");
        databaseMgr = CoreDatabaseManager.open(options);
    }

    @Given("typedb stops")
    public void typedb_stops() {
        after();
    }

    @Given("connection opens with default authentication")
    public void connection_opens_with_default_authentication() {
        // no-op for embedded server
    }

    @Given("connection opens with authentication: {word}, {word}")
    public void connection_opens_with_authentication(String usernamne, String password) {
        // no-op for embedded server
    }

    @Given("connection has been opened")
    public void connection_has_been_opened() {
        assertNotNull(databaseMgr);
        assertTrue(databaseMgr.isOpen());
    }

    @Given("connection closes")
    public void connection_closes() {
        // no-op for embedded server
    }

    @Given("connection does not have any database")
    public void connection_does_not_have_any_database() {
        assertTrue(databaseMgr.all().isEmpty());
    }

    private static void resetDirectory() throws IOException {
        if (Files.exists(dataDir)) {
            System.out.println("Database directory exists!");
            Files.walk(dataDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            System.out.println("Database directory deleted!");
        }

        Files.createDirectory(dataDir);
        System.out.println("Database Directory created: " + dataDir.toString());
    }
}
