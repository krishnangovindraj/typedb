/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.database;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.JAVA_ERROR;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SCHEMA_ACQUIRE_LOCK_TIMEOUT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SESSION_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.DATA_ACQUIRE_LOCK_TIMEOUT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class CoreSession implements TypeDB.Session {

    private final CoreDatabase database;
    private final UUID uuid;
    private final Context.Session context;
    protected final ConcurrentMap<CoreTransaction, Long> transactions;
    protected final AtomicBoolean isOpen;

    private CoreSession(CoreDatabase database, Arguments.Session.Type type, Options.Session options) {
        this.database = database;
        this.context = new Context.Session(database.options(), options).type(type);

        uuid = UUID.randomUUID();
        transactions = new ConcurrentHashMap<>();
        isOpen = new AtomicBoolean(true);
    }

    public Context.Session context() {
        return context;
    }

    boolean isSchema() {
        return false;
    }

    boolean isData() {
        return false;
    }

    CoreSession.Schema asSchema() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(CoreSession.Schema.class));
    }

    CoreSession.Data asData() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(CoreSession.Data.class));
    }

    abstract void closed(CoreTransaction transaction);

    @Override
    public Arguments.Session.Type type() {
        return context.sessionType();
    }

    @Override
    public CoreDatabase database() {
        return database;
    }

    @Override
    public abstract CoreTransaction transaction(Arguments.Transaction.Type type);

    @Override
    public abstract CoreTransaction transaction(Arguments.Transaction.Type type, Options.Transaction options);

    @Override
    public UUID uuid() {
        return uuid;
    }

    @Override
    public boolean isOpen() {
        return this.isOpen.get();
    }

    @Override
    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            transactions.keySet().parallelStream().forEach(CoreTransaction::close);
            database().closed(this);
        }
    }

    public static class Schema extends CoreSession {
        protected final Factory.TransactionSchema txSchemaFactory;
        protected final Lock writeLock;

        public Schema(CoreDatabase database, Arguments.Session.Type type, Options.Session options,
                      Factory.TransactionSchema txSchemaFactory) {
            super(database, type, options);
            this.txSchemaFactory = txSchemaFactory;
            this.writeLock = new StampedLock().asReadWriteLock().writeLock();
        }

        @Override
        boolean isSchema() {
            return true;
        }

        @Override
        CoreSession.Schema asSchema() {
            return this;
        }

        protected CoreTransaction.Schema initialisationTransaction() {
            if (!isOpen.get()) throw TypeDBException.of(SESSION_CLOSED);
            try {
                if (!writeLock.tryLock(new Options.Transaction().schemaLockTimeoutMillis(), MILLISECONDS)) {
                    throw TypeDBException.of(SCHEMA_ACQUIRE_LOCK_TIMEOUT);
                }
            } catch (InterruptedException e) {
                throw TypeDBException.of(JAVA_ERROR, e);
            }
            CoreTransaction.Schema transaction = txSchemaFactory.initialisationTransaction(this);
            transactions.put(transaction, 0L);
            return transaction;
        }

        @Override
        public CoreTransaction.Schema transaction(Arguments.Transaction.Type type) {
            return transaction(type, new Options.Transaction());
        }

        @Override
        public CoreTransaction.Schema transaction(Arguments.Transaction.Type type, Options.Transaction options) {
            if (!isOpen.get()) throw TypeDBException.of(SESSION_CLOSED);
            if (type.isWrite()) {
                try {
                    if (!writeLock.tryLock(options.schemaLockTimeoutMillis(), MILLISECONDS)) {
                        throw TypeDBException.of(SCHEMA_ACQUIRE_LOCK_TIMEOUT);
                    }
                } catch (InterruptedException e) {
                    throw TypeDBException.of(JAVA_ERROR, e);
                }
            }
            CoreTransaction.Schema transaction = txSchemaFactory.transaction(this, type, options);
            transactions.put(transaction, 0L);
            return transaction;

        }

        @Override
        void closed(CoreTransaction transaction) {
            transactions.remove(transaction);
            if (transaction.type().isWrite()) writeLock.unlock();
        }
    }

    public static class Data extends CoreSession {

        protected final Factory.TransactionData txDataFactory;

        public Data(CoreDatabase database, Arguments.Session.Type type, Options.Session options, Factory.TransactionData txDataFactory) {
            super(database, type, options);
            this.txDataFactory = txDataFactory;
        }

        @Override
        boolean isData() {
            return true;
        }

        @Override
        CoreSession.Data asData() {
            return this;
        }

        @Override
        public CoreTransaction.Data transaction(Arguments.Transaction.Type type) {
            return transaction(type, new Options.Transaction());
        }

        @Override
        public CoreTransaction.Data transaction(Arguments.Transaction.Type type, Options.Transaction options) {
            if (!isOpen.get()) throw TypeDBException.of(SESSION_CLOSED);
            long lock = 0;
            if (type == Arguments.Transaction.Type.WRITE) {
                try {
                    long timeout = options.schemaLockTimeoutMillis();
                    lock = database().schemaLock().tryReadLock(timeout, MILLISECONDS);
                    if (lock == 0) throw TypeDBException.of(DATA_ACQUIRE_LOCK_TIMEOUT);
                } catch (InterruptedException e) {
                    throw TypeDBException.of(UNEXPECTED_INTERRUPTION);
                }
            }
            CoreTransaction.Data transaction = txDataFactory.transaction(this, type, options);
            transactions.put(transaction, lock);
            return transaction;
        }

        @Override
        void closed(CoreTransaction transaction) {
            long lock = transactions.remove(transaction);
            if (transaction.type().isWrite()) {
                assert lock != 0;
                database().schemaLock().unlockRead(lock);
            }
        }
    }
}