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

package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.core.common.cache.CommonCache;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.controller.ConcludableController;
import com.vaticle.typedb.core.reasoner.common.ReasonerPerfCounters;
import com.vaticle.typedb.core.traversal.TraversalEngine;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.intersection;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ReasonerPlanner {
    final ConceptManager conceptMgr;
    final TraversalEngine traversalEng;
    final LogicManager logicMgr;
    private final boolean explain;
    final CommonCache<CallMode, Plan> planCache;
    final ReasonerPerfCounters perfCounters;

    public ReasonerPlanner(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr, ReasonerPerfCounters perfCounters, boolean explain) {
        this.traversalEng = traversalEng;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.perfCounters = perfCounters;
        this.explain = explain;
        this.planCache = new CommonCache<>();
    }

    public static ReasonerPlanner create(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr, ReasonerPerfCounters perfCounters, boolean explain) {
        return RecursivePlanner.create(traversalEng, conceptMgr, logicMgr, perfCounters, explain);
    }

    static Set<Variable> estimateableVariables(Set<Variable> variables) {
        return iterate(variables).filter(Variable::isThing).toSet();
    }

    static boolean dependenciesSatisfied(Resolvable<?> resolvable, Set<Variable> boundVars, Map<Resolvable<?>, Set<Variable>> dependencies) {
        return boundVars.containsAll(dependencies.get(resolvable));
    }

    public void plan(ResolvableConjunction conjunction, Set<Variable> mode) {
        long start = System.nanoTime();
        plan(new CallMode(conjunction, estimateableVariables(mode)));
        perfCounters.timePlanning.add(System.nanoTime() - start);
    }

    public void planAllDependencies(Concludable concludable, Set<Variable> mode) {
        triggeredCalls(concludable, estimateableVariables(mode), null)
                .forEach(callMode -> plan(callMode));
    }

    public Plan getPlan(ResolvableConjunction conjunction, Set<Variable> mode) {
        return getPlan(new CallMode(conjunction, estimateableVariables(mode)));
    }

    synchronized void plan(CallMode callMode) {
        if (planCache.getIfPresent(callMode) == null) {
            planCache.put(callMode, computePlan(callMode));
        }
    }

    Plan getPlan(CallMode callMode) {
        assert planCache.getIfPresent(callMode) != null;
        return planCache.getIfPresent(callMode);
    }

    abstract Plan computePlan(CallMode callMode);

    /**
     * Determine the resolvables that are dependent upon the generation of each variable
     */
    static Map<Resolvable<?>, Set<Variable>> dependencies(Set<Resolvable<?>> resolvables) {
        // TODO: There may not be any generated->used dependencies since every use either triggers the rule or is unsatisfiable
        Map<Resolvable<?>, Set<Variable>> deps = new HashMap<>();
        iterate(resolvables).forEachRemaining(resolvable -> deps.put(resolvable, new HashSet<>()));

        // Add dependency between negateds and shared variables
        Set<Variable> unnegatedVars = iterate(resolvables).filter(r -> !r.isNegated())
                .flatMap(r -> iterate(r.variables()).filter(v -> !(v.isType() && v.reference().isLabel())))
                .toSet();
        iterate(resolvables).filter(Resolvable::isNegated)
                .forEachRemaining(resolvable -> deps.get(resolvable).addAll(intersection(resolvable.variables(), unnegatedVars)));
        Set<ThingVariable> generatedVars = iterate(resolvables).filter(resolvable -> resolvable.generating().isPresent())
                .map(resolvable -> resolvable.generating().get()).toSet();
        // Add dependency for resolvable to any variables for which it can't (independently) generate all satisfying values
        iterate(resolvables).filter(resolvable -> !resolvable.isNegated())
                .forEachRemaining(resolvable -> deps.get(resolvable).addAll(
                        iterate(resolvable.variables()).filter(v -> generatedVars.contains(v) && notGeneratedByResolvable(resolvable, v)).toSet()
                ));

        return deps;
    }

    private static boolean notGeneratedByResolvable(Resolvable<?> resolvable, Variable variable) {
        return !resolvable.generating().map(generating -> generating.equals(variable)).orElse(false) &&
                Iterators.link(iterate(variable.constraints()), iterate(variable.constraining()))
                .allMatch(constraint -> constraint.isThing() && constraint.asThing().isValue());
    }

    public Set<CallMode> triggeredCalls(Concludable concludable, Set<Variable> mode, @Nullable Set<ResolvableConjunction> dependencyFilter) {
        Set<CallMode> calls = new HashSet<>();

        if (ConcludableController.canBypassReasoning(concludable, iterate(mode).map(v -> v.id().asRetrievable()).toSet(), explain)) {
            return calls;
        }

        for (Map.Entry<Rule, Set<Unifier>> entry : logicMgr.applicableRules(concludable).entrySet()) {
            for (Rule.Condition.ConditionBranch conditionBranch : entry.getKey().condition().branches()) {
                ResolvableConjunction ruleConjunction = conditionBranch.conjunction();
                if (dependencyFilter != null && !dependencyFilter.contains(ruleConjunction)) {
                    continue;
                }
                for (Unifier unifier : entry.getValue()) {
                    assert iterate(mode).allMatch(v -> v.id().isRetrievable());
                    Set<Variable> ruleMode = iterate(mode)
                            .flatMap(v -> iterate(unifier.mapping().get(v.id().asRetrievable())))
                            .filter(id -> ruleConjunction.pattern().retrieves().contains(id))
                            .map(id -> ruleConjunction.pattern().variable(id)).toSet();
                    calls.add(new CallMode(ruleConjunction, ruleMode));
                }
            }
        }
        return calls;
    }

    static class CallMode {
        final ResolvableConjunction conjunction;
        final Set<Variable> mode;
        private final int hash;

        CallMode(ResolvableConjunction conjunction, Set<Variable> mode) {
            this.conjunction = conjunction;
            this.mode = mode;
            this.hash = Objects.hash(conjunction, mode);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CallMode that = (CallMode) o;
            return conjunction.equals(that.conjunction) && mode.equals(that.mode);
        }

        @Override
        public String toString() {
            return conjunction + "::{" + iterate(mode).reduce("", (x, y) -> (x + ", " + y)) + "}";
        }
    }

    public static class Plan {
        private final List<Resolvable<?>> order;
        private final CallMode callMode;
        private final double allCallsCost;
        // fraction of all calls triggered by cycles
        public double cyclicScalingFactor;

        public Plan(List<Resolvable<?>> resolvableOrder, CallMode callMode, double allCallsCost, double cyclicScalingFactor) {
            this.callMode = callMode;
            this.order = resolvableOrder;
            this.allCallsCost = allCallsCost;
            this.cyclicScalingFactor = cyclicScalingFactor;
        }

        public List<Resolvable<?>> plan() {
            return order;
        }

        public double allCallsCost() {
            return allCallsCost;
        }
    }
}
