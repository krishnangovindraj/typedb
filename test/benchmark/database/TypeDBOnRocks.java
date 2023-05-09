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
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typeql.lang.TypeQL;

import java.nio.file.Path;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;

public class TypeDBOnRocks implements DatabaseBenchmark.TestSubject {

    private TypeDB.Session activeSession;
    private TypeDB.Transaction activeTransaction;

    private CoreDatabaseManager databaseMgr;
    private AttributeType.Long vertexType;

    @Override
    public void setup(Path dataDir, String database) {
        Options.Database options = new Options.Database().dataDir(dataDir)
                .storageDataCacheSize(MB).storageIndexCacheSize(MB);

        databaseMgr = CoreDatabaseManager.open(options);
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
        activeTransaction.close();
        activeSession.close();
    }


    @Override
    public void openTransaction() {
        activeTransaction = activeSession.transaction(Arguments.Transaction.Type.WRITE);
        vertexType = activeTransaction.concepts().getAttributeType("vertex").asLong();
    }

    @Override
    public void commit() {
        activeTransaction.commit();
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
