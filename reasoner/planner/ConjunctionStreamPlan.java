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
 */

package com.vaticle.typedb.core.reasoner.planner;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.*;

import static com.vaticle.typedb.common.collection.Collections.*;
import static com.vaticle.typedb.common.collection.Collections.intersection;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.util.Collections.emptySet;

public abstract class ConjunctionStreamPlan {
    protected final Set<Identifier.Variable.Retrievable> identifierVariables;
    protected final Set<Identifier.Variable.Retrievable> extensionVariables;
    protected final Set<Identifier.Variable.Retrievable> outputVariables;

    public ConjunctionStreamPlan(Set<Identifier.Variable.Retrievable> identifierVariables, Set<Identifier.Variable.Retrievable> extensionVariables, Set<Identifier.Variable.Retrievable> outputVariables) {
        this.identifierVariables = identifierVariables;
        this.extensionVariables = extensionVariables;
        this.outputVariables = outputVariables;
    }

    public static ConjunctionStreamPlan createUnflattened(List<Resolvable<?>> resolvableOrder, Set<Identifier.Variable.Retrievable> inputVariables, Set<Identifier.Variable.Retrievable> outputVariables) {
        Builder builder = new Builder(resolvableOrder, inputVariables, outputVariables);
        return builder.build();
    }

    public static ConjunctionStreamPlan create(List<Resolvable<?>> resolvableOrder, Set<Identifier.Variable.Retrievable> inputVariables, Set<Identifier.Variable.Retrievable> outputVariables) {
        Builder builder = new Builder(resolvableOrder, inputVariables, outputVariables);
        return builder.flatten(builder.build());
    }

    public boolean isResolvablePlan() {
        return false;
    }

    public boolean isCompoundStreamPlan() {
        return false;
    }

    public ResolvablePlan asResolvablePlan() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), className(ResolvablePlan.class));
    }

    public CompoundStreamPlan asCompoundStreamPlan() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), className(CompoundStreamPlan.class));
    }

    public Set<Identifier.Variable.Retrievable> outputs() {
        return outputVariables;
    }

    public Set<Identifier.Variable.Retrievable> identifiers() {
        return identifierVariables;
    }

    public Set<Identifier.Variable.Retrievable> extensions() {
        return extensionVariables;
    }

    public abstract boolean mayProduceDuplicates();

    public static class ResolvablePlan extends ConjunctionStreamPlan {
        private final Resolvable<?> resolvable;
        private final boolean mayProduceDuplicates;

        public ResolvablePlan(Resolvable<?> resolvable, Set<Identifier.Variable.Retrievable> identifierVariables, Set<Identifier.Variable.Retrievable> extendOutputWith, Set<Identifier.Variable.Retrievable> outputVariables) {
            super(identifierVariables, extendOutputWith, outputVariables);
            this.resolvable = resolvable;
            this.mayProduceDuplicates = !concatToSet(identifierVariables, extendOutputWith).containsAll(resolvable.retrieves());
        }

        @Override
        public boolean isResolvablePlan() {
            return true;
        }

        @Override
        public ResolvablePlan asResolvablePlan() {
            return this;
        }

        @Override
        public boolean mayProduceDuplicates() {
            return mayProduceDuplicates;
        }

        public Resolvable<?> resolvable() {
            return resolvable;
        }

        @Override
        public String toString() {
            return String.format("{[(%s), (%s), (%s)] :: Resolvable(%s)}",
                    String.join(", ", iterate(identifierVariables).map(Identifier.Variable::toString).toList()),
                    String.join(", ", iterate(extensionVariables).map(Identifier.Variable::toString).toList()),
                    String.join(", ", iterate(outputVariables).map(Identifier.Variable::toString).toList()),
                    resolvable.toString()
            );
        }
    }

    public static class CompoundStreamPlan extends ConjunctionStreamPlan {
        private final List<ConjunctionStreamPlan> childPlan;
        private final boolean mayProduceDuplicates;
        private final Map<ConjunctionStreamPlan, Boolean> isExclusiveReaderOfChild;

        public CompoundStreamPlan(List<ConjunctionStreamPlan> childPlan,
                                  Set<Identifier.Variable.Retrievable> identifierVariables, Set<Identifier.Variable.Retrievable> extendOutputWith,
                                  Set<Identifier.Variable.Retrievable> outputVariables,
                                  Map<ConjunctionStreamPlan, Boolean> isExclusiveReaderOfChild) {
            super(identifierVariables, extendOutputWith, outputVariables);
            assert childPlan.size() > 1;
            this.childPlan = childPlan;
            this.mayProduceDuplicates = childPlan.get(childPlan.size() - 1).isResolvablePlan() &&
                    childPlan.get(childPlan.size() - 1).asResolvablePlan().mayProduceDuplicates();
            this.isExclusiveReaderOfChild = isExclusiveReaderOfChild;
        }

        @Override
        public boolean isCompoundStreamPlan() {
            return true;
        }

        @Override
        public CompoundStreamPlan asCompoundStreamPlan() {
            return this;
        }

        @Override
        public boolean mayProduceDuplicates() {
            return mayProduceDuplicates;
        }

        public ConjunctionStreamPlan childAt(int i) {
            return childPlan.get(i);
        }

        public int size() {
            return childPlan.size();
        }

        @Override
        public String toString() {
            return String.format("{[(%s), (%s), (%s)] :: [%s]}",
                    String.join(", ", iterate(identifierVariables).map(Identifier.Variable::toString).toList()),
                    String.join(", ", iterate(extensionVariables).map(Identifier.Variable::toString).toList()),
                    String.join(", ", iterate(outputVariables).map(Identifier.Variable::toString).toList()),
                    String.join(" ; ", iterate(childPlan).map(ConjunctionStreamPlan::toString).toList()));
        }

        public boolean mustBufferAnswers(ConjunctionStreamPlan child) {
            return child.mayProduceDuplicates() || !isExclusiveReaderOfChild(child);
        }

        private boolean isExclusiveReaderOfChild(ConjunctionStreamPlan child) {
            return isExclusiveReaderOfChild.getOrDefault(child, false);
        }
    }

    private static class Builder {
        private final List<Resolvable<?>> resolvables;
        private final Set<Identifier.Variable.Retrievable> processorInputs;
        private final Set<Identifier.Variable.Retrievable> processorOutputs;

        private final List<Set<Identifier.Variable.Retrievable>> boundsBefore;

        private Builder(List<Resolvable<?>> resolvables, Set<Identifier.Variable.Retrievable> processorInputs, Set<Identifier.Variable.Retrievable> processorOutputs) {
            this.resolvables = resolvables;
            this.processorInputs = processorInputs;
            this.processorOutputs = processorOutputs;
            boundsBefore = new ArrayList<>();
            Set<Identifier.Variable.Retrievable> runningBounds = new HashSet<>(processorInputs);
            for (Resolvable<?> resolvable : resolvables) {
                boundsBefore.add(new HashSet<>(runningBounds));
                runningBounds.addAll(resolvable.retrieves());
            }
        }

        private ConjunctionStreamPlan build() {
            return buildPrefix(resolvables, processorInputs, processorOutputs);
        }

        public ConjunctionStreamPlan buildPrefix(List<Resolvable<?>> prefix, Set<Identifier.Variable.Retrievable> availableInputs, Set<Identifier.Variable.Retrievable> requiredOutputs) {
            if (prefix.size() == 1) {
                VariableSets variableSets = VariableSets.create(list(), prefix, availableInputs, requiredOutputs);
                //  use resolvableOutputs instead of rightOutputs because this node has to do the job of the parent as well - joining the identifiers
                Set<Identifier.Variable.Retrievable> resolvableOutputs = difference(requiredOutputs, variableSets.extensions);
                return new ResolvablePlan(prefix.get(0), variableSets.rightInputs, variableSets.extensions, resolvableOutputs);
            } else {
                Pair<List<Resolvable<?>>, List<Resolvable<?>>> divided = divide(prefix);
                VariableSets variableSets = VariableSets.create(divided.first(), divided.second(), availableInputs, requiredOutputs);
                ConjunctionStreamPlan leftPlan = buildPrefix(divided.first(), variableSets.leftIdentifiers, variableSets.leftOutputs);
                ConjunctionStreamPlan rightPlan = buildSuffix(divided.second(), variableSets.rightInputs, variableSets.rightOutputs);
                return new CompoundStreamPlan(list(leftPlan, rightPlan), variableSets.identifiers, variableSets.extensions, requiredOutputs, new HashMap<>());
            }
        }

        public ConjunctionStreamPlan buildSuffix(List<Resolvable<?>> suffix, Set<Identifier.Variable.Retrievable> availableInputs, Set<Identifier.Variable.Retrievable> requiredOutputs) {
            if (suffix.size() == 1) {
                VariableSets variableSets = VariableSets.create(list(), suffix, availableInputs, requiredOutputs);
                Set<Identifier.Variable.Retrievable> resolvableOutputs = difference(requiredOutputs, variableSets.extensions);
                return new ResolvablePlan(suffix.get(0), variableSets.rightInputs, variableSets.extensions, resolvableOutputs);
            } else {
                List<Resolvable<?>> nextSuffix = suffix.subList(1, suffix.size());
                VariableSets variableSets = VariableSets.create(suffix.subList(0, 1), suffix.subList(1, suffix.size()), availableInputs, requiredOutputs);
                ConjunctionStreamPlan leftPlan = new ResolvablePlan(suffix.get(0), variableSets.leftIdentifiers, emptySet(), variableSets.leftOutputs);
                ConjunctionStreamPlan rightPlan = buildSuffix(nextSuffix, variableSets.rightInputs, variableSets.rightOutputs);
                return new CompoundStreamPlan(list(leftPlan, rightPlan), variableSets.identifiers, variableSets.extensions, requiredOutputs, new HashMap<>());
            }
        }

        public Pair<List<Resolvable<?>>, List<Resolvable<?>>> divide(List<Resolvable<?>> resolvables) {
            int splitAfter;
            Set<Identifier.Variable.Retrievable> suffixVars = new HashSet<>(resolvables.get(resolvables.size() - 1).retrieves());
            for (splitAfter = resolvables.size() - 2; splitAfter > 0; splitAfter--) {
                suffixVars.addAll(resolvables.get(splitAfter).retrieves());
                Set<Identifier.Variable.Retrievable> suffixBounds = intersection(boundsBefore.get(splitAfter), suffixVars);
                Set<Identifier.Variable.Retrievable> a = resolvables.get(splitAfter).retrieves();
                Set<Identifier.Variable.Retrievable> suffixFirstResolvableBounds = intersection(a, boundsBefore.get(splitAfter));
                if (!suffixFirstResolvableBounds.equals(suffixBounds)) {
                    break;
                }
            }
            assert splitAfter >= 0;
            return new Pair<>(resolvables.subList(0, splitAfter + 1), resolvables.subList(splitAfter + 1, resolvables.size()));
        }

        private ConjunctionStreamPlan flatten(ConjunctionStreamPlan plan) {
            if (plan.isResolvablePlan()) {
                return plan;
            } else {
                CompoundStreamPlan compoundPlan = plan.asCompoundStreamPlan();
                assert compoundPlan.size() == 2;
                List<ConjunctionStreamPlan> childPlans = new ArrayList<>();
                for (int i = 0; i < 2; i++) {
                    ConjunctionStreamPlan child = compoundPlan.childAt(i);
                    if (child.isCompoundStreamPlan() && canFlattenInto(compoundPlan, child.asCompoundStreamPlan())) {
                        childPlans.addAll(flatten(child).asCompoundStreamPlan().childPlan);
                    } else {
                        childPlans.add(flatten(child));
                    }
                }

                Map<ConjunctionStreamPlan, Boolean> isExclusiveReaderOfChild = new HashMap<>();
                for (ConjunctionStreamPlan child : childPlans) {
                    boolean exclusivelyReads = child.isResolvablePlan() ||  // We don't re-use resolvable plans
                            isExclusiveReader(compoundPlan, child.asCompoundStreamPlan(), processorOutputs);
                    isExclusiveReaderOfChild.put(child, exclusivelyReads);
                }

                return new CompoundStreamPlan(childPlans, compoundPlan.identifiers(), compoundPlan.extensions(), compoundPlan.outputs(), isExclusiveReaderOfChild);
            }
        }

        private boolean canFlattenInto(CompoundStreamPlan parent, CompoundStreamPlan childToFlatten) {
            return isExclusiveReader(parent, childToFlatten, processorInputs) &&
                    boundsRemainSatisfied(parent, childToFlatten);
        }

        private static boolean isExclusiveReader(CompoundStreamPlan parent, CompoundStreamPlan child, Set<Identifier.Variable.Retrievable> processorBounds) {
            return child.extensions().isEmpty() && child.identifierVariables.containsAll(difference(parent.identifierVariables, processorBounds));
        }

        private static boolean boundsRemainSatisfied(CompoundStreamPlan parent, CompoundStreamPlan childToFlatten) {
            return difference(
                    childToFlatten.childAt(1).identifierVariables,
                    union(parent.identifierVariables, childToFlatten.childAt(0).outputVariables))
                    .isEmpty() &&
                    union(
                            childToFlatten.asCompoundStreamPlan().childAt(1).outputs(),
                            childToFlatten.asCompoundStreamPlan().childAt(1).extensions())
                            .equals(childToFlatten.outputs());
        }

        private static class VariableSets {

            public final Set<Identifier.Variable.Retrievable> identifiers;
            public final Set<Identifier.Variable.Retrievable> extensions;
            public final Set<Identifier.Variable.Retrievable> requiredOutputs;
            public final Set<Identifier.Variable.Retrievable> leftIdentifiers;
            public final Set<Identifier.Variable.Retrievable> leftOutputs;
            public final Set<Identifier.Variable.Retrievable> rightInputs;
            public final Set<Identifier.Variable.Retrievable> rightOutputs;

            private VariableSets(Set<Identifier.Variable.Retrievable> identifiers, Set<Identifier.Variable.Retrievable> extensions, Set<Identifier.Variable.Retrievable> requiredOutputs,
                                 Set<Identifier.Variable.Retrievable> leftIdentifiers, Set<Identifier.Variable.Retrievable> leftOutputs,
                                 Set<Identifier.Variable.Retrievable> rightInputs, Set<Identifier.Variable.Retrievable> rightOutputs) {
                this.identifiers = identifiers;
                this.extensions = extensions;
                this.requiredOutputs = requiredOutputs;
                this.leftIdentifiers = leftIdentifiers;
                this.leftOutputs = leftOutputs;
                this.rightInputs = rightInputs;
                this.rightOutputs = rightOutputs;
            }

            private static VariableSets create(List<Resolvable<?>> left, List<Resolvable<?>> right, Set<Identifier.Variable.Retrievable> availableInputs, Set<Identifier.Variable.Retrievable> requiredOutputs) {
                Set<Identifier.Variable.Retrievable> leftVariables = iterate(left).flatMap(resolvable -> iterate(resolvable.retrieves())).toSet();
                Set<Identifier.Variable.Retrievable> rightVariables = iterate(right).flatMap(resolvable -> iterate(resolvable.retrieves())).toSet();
                Set<Identifier.Variable.Retrievable> allUsedVariables = union(leftVariables, rightVariables);

                Set<Identifier.Variable.Retrievable> identifiers = intersection(availableInputs, allUsedVariables);
                Set<Identifier.Variable.Retrievable> extensions = difference(availableInputs, allUsedVariables);
                Set<Identifier.Variable.Retrievable> rightOutputs = difference(requiredOutputs, availableInputs);

                Set<Identifier.Variable.Retrievable> leftIdentifiers = intersection(identifiers, leftVariables);
                Set<Identifier.Variable.Retrievable> a = union(identifiers, leftVariables);
                Set<Identifier.Variable.Retrievable> b = union(rightVariables, rightOutputs);
                Set<Identifier.Variable.Retrievable> rightInputs = intersection(a, b);
                Set<Identifier.Variable.Retrievable> leftOutputs = difference(rightInputs, difference(identifiers, leftIdentifiers));

                return new VariableSets(identifiers, extensions, requiredOutputs, leftIdentifiers, leftOutputs, rightInputs, rightOutputs);
            }

        }
    }

    public static Set<Identifier.Variable.Retrievable> union(Set<Identifier.Variable.Retrievable> a, Set<Identifier.Variable.Retrievable> b) {
        Set<Identifier.Variable.Retrievable> result = new HashSet<>(a);
        result.addAll(b);
        return result;
    }

    private static Set<Identifier.Variable.Retrievable> difference(Set<Identifier.Variable.Retrievable> a, Set<Identifier.Variable.Retrievable> b) {
        Set<Identifier.Variable.Retrievable> result = new HashSet<>(a);
        result.removeAll(b);
        return result;
    }
}
