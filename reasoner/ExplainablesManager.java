/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner;

import com.vaticle.typedb.core.common.cache.CommonCache;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Pattern;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.concept.answer.ConceptMap.Explainable.NOT_IDENTIFIED;

public class ExplainablesManager {

    private final AtomicLong nextId;
    private final ConcurrentMap<Long, Concludable> concludables;
    private final ConcurrentMap<Long, ConceptMap> bounds;

    ExplainablesManager() {
        this.nextId = new AtomicLong(NOT_IDENTIFIED + 1);
        this.concludables = new ConcurrentHashMap<>();
        this.bounds = new ConcurrentHashMap<>();
        this.patternBounds = new HashMap<>();
    }

    public void setAndRecordExplainables(ConceptMap explainableMap) {
        explainableMap.explainables().iterator().forEachRemaining(explainable -> {
            long nextId = this.nextId.getAndIncrement();
            FunctionalIterator<Concludable> concludable = iterate(Concludable.create(explainable.conjunction()));
            assert concludable.hasNext();
            concludables.put(nextId, concludable.next());
            assert !concludable.hasNext();
            bounds.put(nextId, explainableMap);
            explainable.setId(nextId);
        });
    }

    Concludable getConcludable(long explainableId) {
        return concludables.get(explainableId);
    }

    ConceptMap getBounds(long explainableId) {
        return bounds.get(explainableId);
    }

    // Added in v4
    private final Map<Conjunction, Set<Identifier.Variable.Retrievable>> patternBounds;
    public void recordBounds(Conjunction pattern, Set<Identifier.Variable.Retrievable> bounds) {
        patternBounds.put(pattern, bounds);
    }

    public Set<Identifier.Variable.Retrievable> getBounds(Conjunction pattern) {
        return patternBounds.get(pattern);
    }
}
