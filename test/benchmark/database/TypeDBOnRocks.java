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

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typeql.lang.TypeQL;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class TypeDBOnRocks implements DatabaseBenchmark.TestSubject {

    private CoreDatabaseManager databaseMgr;

    private TypeDB.Session activeSession;
    private TypeDB.Transaction activeTransaction;
    private AttributeType.Long vertexType;
    private final Options.Database databaseOptions;

    public TypeDBOnRocks(Options.Database databaseOptions) {
        this.databaseOptions = databaseOptions;
    }

    @Override
    public void setup(String database) {
        databaseMgr = CoreDatabaseManager.open(databaseOptions);
        databaseMgr.create(database);

        try (TypeDB.Session schemaSession = databaseMgr.session(database, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = schemaSession.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().define(
                        TypeQL.parseQuery("define\n" +
                                "vertex sub attribute, value long, owns vertex;\n"
                        ).asDefine());
                tx.commit();
            }
        }

        activeSession = databaseMgr.session(database, Arguments.Session.Type.DATA);
        activeTransaction = null;
    }

    @Override
    public void tearDown() {
        if (activeTransaction != null) activeTransaction.close();
        activeTransaction = null;
        if (activeSession != null) activeSession.close();
        activeSession = null;
        databaseMgr.close();
    }


    @Override
    public void openWriteTransaction() {
        if (activeTransaction != null) throw TypeDBException.of(ILLEGAL_STATE);
        activeTransaction = activeSession.transaction(Arguments.Transaction.Type.WRITE);
        vertexType = activeTransaction.concepts().getAttributeType("vertex").asLong();
    }

    @Override
    public void commitWrites() {
        activeTransaction.commit();
        activeTransaction = null;
    }

    @Override
    public void openReadTransaction() {
        if (activeTransaction != null) throw TypeDBException.of(ILLEGAL_STATE);
        activeTransaction = activeSession.transaction(Arguments.Transaction.Type.READ);
        vertexType = activeTransaction.concepts().getAttributeType("vertex").asLong();
    }

    @Override
    public void closeRead() {
        if (activeTransaction != null) activeTransaction.close();
        activeTransaction = null;
    }

    @Override
    public void insertVertex(long id) {
        vertexType.put(id);
    }

    @Override
    public void insertEdge(long from, long to) {
        vertexType.get(from).setHas(vertexType.get(to));
    }

    @Override
    public FunctionalIterator<Long> queryEdges(long from) {
        return vertexType.get(from).getHas(vertexType).map(Attribute.Long::getValue);
    }
}
