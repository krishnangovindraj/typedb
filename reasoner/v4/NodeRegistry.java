package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController;
import com.vaticle.typedb.core.reasoner.planner.ReasonerPlanner;
import com.vaticle.typedb.core.reasoner.planner.RecursivePlanner;
import com.vaticle.typedb.core.reasoner.v4.nodes.ConjunctionNode;
import com.vaticle.typedb.core.reasoner.v4.nodes.ResolvableNode;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

// TODO: See if we can use ConjunctionStreamPlan as the only one key we need. The nodes can do safe-downcasting
public class NodeRegistry {
    private final ActorExecutorGroup executorService;
    private final Map<ResolvableConjunction, ConjunctionSubRegistry> conjunctionSubRegistries;
    private final Map<Retrievable, RetrievableSubRegistry> retrievableSubRegistries;
    private final Map<Concludable, ConcludableSubRegistry> concludableSubRegistries;
    private final Set<ActorNode<?>> roots;
    private final LogicManager logicManager;
    private final RecursivePlanner planner;
    private AtomicBoolean terminated;
    private final TraversalEngine traversalEngine;
    private final ConceptManager conceptManager;

    public NodeRegistry(ActorExecutorGroup executorService,
                        ConceptManager conceptManager, LogicManager logicManager, TraversalEngine traversalEngine,
                        ReasonerPlanner planner) {
        this.executorService = executorService;
        this.traversalEngine = traversalEngine;
        this.conceptManager = conceptManager;
        this.logicManager = logicManager;
        this.planner = planner.asRecursivePlanner();
        this.conjunctionSubRegistries = new ConcurrentHashMap<>();
        this.retrievableSubRegistries = new ConcurrentHashMap<>();
        this.concludableSubRegistries = new ConcurrentHashMap<>();
        this.roots = new ConcurrentSet<>();
        this.terminated = new AtomicBoolean(false);
    }

    void prepare(ResolvableConjunction conjunction, ConceptMap bounds) {
        Set<Variable> boundVars = Iterators.iterate(bounds.concepts().keySet()).map(id -> conjunction.pattern().variable(id)).toSet();
        planner.plan(conjunction, boundVars);
        populateRegistries(conjunction);
    }

    private void populateRegistries(ResolvableConjunction conjunction) {
        if (!concludableSubRegistries.containsKey(conjunction)) {
            conjunctionSubRegistries.put(conjunction, new ConjunctionSubRegistry(conjunction));
            logicManager().compile(conjunction).forEach(resolvable -> {
                if (resolvable.isConcludable()) {
                    concludableSubRegistries.computeIfAbsent(resolvable.asConcludable(), ConcludableSubRegistry::new);
                    planner.conjunctionGraph().dependencies(resolvable.asConcludable()).forEach(dependencyConjunction -> {
                        populateRegistries(dependencyConjunction);
                    });
                } else if (resolvable.isRetrievable()) {
                    retrievableSubRegistries.computeIfAbsent(resolvable.asRetrievable(), RetrievableSubRegistry::new);
                } else if (resolvable.isNegated()) {
                    throw TypeDBException.of(UNIMPLEMENTED);
                } else throw TypeDBException.of(ILLEGAL_STATE);
            });
        }
    }

    public ConjunctionSubRegistry conjunctionSubRegistry(ResolvableConjunction conjunction) {
        return conjunctionSubRegistries.computeIfAbsent(conjunction, conj -> new ConjunctionSubRegistry(conj));
    }

    public RetrievableSubRegistry retrievableSubRegistry(Retrievable retrievable) {
        return retrievableSubRegistries.computeIfAbsent(retrievable, res -> new RetrievableSubRegistry(res));
    }

    public ConcludableSubRegistry concludableSubRegistry(Concludable concludable) {
        return concludableSubRegistries.computeIfAbsent(concludable, con -> new ConcludableSubRegistry(con));
    }

    public <NODE extends ActorNode<NODE>> NODE createRoot(Function<Actor.Driver<NODE>, NODE> actorFn) {
        Actor.Driver<NODE> driver = createDriver(actorFn);
        this.roots.add(driver.actor());
        return driver.actor();
    }

    private <NODE extends ActorNode<NODE>> Actor.Driver<NODE> createDriver(Function<Actor.Driver<NODE>, NODE> actorFn) {
        return Actor.driver(actorFn, executorService);
    }

    public void terminate(Throwable e) {
        if (terminated.compareAndSet(false, true)) {
            roots.forEach(root -> root.terminate(e));
            conjunctionSubRegistries.values().forEach(subReg -> subReg.terminateAll(e));
            retrievableSubRegistries.values().forEach(subReg -> subReg.terminateAll(e));
        }
    }

    public ConceptManager conceptManager() {
        return conceptManager;
    }

    public LogicManager logicManager() {
        return logicManager;
    }

    public TraversalEngine traversalEngine() {
        return traversalEngine;
    }

    public abstract class SubRegistry<KEY, NODE extends ActorNode<NODE>> {
        protected final KEY key;
        private final Map<ConceptMap, NODE> subRegistry;

        private SubRegistry(KEY key) {
            this.key = key;
            this.subRegistry = new ConcurrentHashMap<>();
        }

        public NODE getNode(ConceptMap bounds) {
            return subRegistry.computeIfAbsent(bounds, b -> createNode(b).actor());
        }

        protected abstract Actor.Driver<NODE> createNode(ConceptMap bounds);

        public void terminateAll(Throwable e) {
            subRegistry.values().forEach(node -> node.terminate(e));
        }
    }

    public class RetrievableSubRegistry extends SubRegistry<Retrievable, ResolvableNode.RetrievableNode> {

        private RetrievableSubRegistry(Retrievable retrievable) {
            super(retrievable);
        }

        @Override
        protected Actor.Driver<ResolvableNode.RetrievableNode> createNode(ConceptMap bounds) {
            return createDriver(driver -> new ResolvableNode.RetrievableNode(key, bounds, NodeRegistry.this, driver));
        }
    }


    public class ConcludableSubRegistry extends SubRegistry<Concludable, ResolvableNode.ConcludableNode> {


        private ConcludableSubRegistry(Concludable concludable) {
            super(concludable);
        }

        @Override
        protected Actor.Driver<ResolvableNode.ConcludableNode> createNode(ConceptMap bounds) {
            return createDriver(driver -> new ResolvableNode.ConcludableNode(key, bounds, NodeRegistry.this, driver));
        }

    }

    public class ConjunctionSubRegistry extends SubRegistry<ResolvableConjunction, ConjunctionNode> {

        private final Map<Set<Identifier.Variable.Retrievable>, ConjunctionController.ConjunctionStreamPlan> conjunctionPlans;
        private final Map<ConjunctionController.ConjunctionStreamPlan, SubConjunctionRegistry> registriesForSubConjunctions;

        private ConjunctionSubRegistry(ResolvableConjunction conjunction) {
            super(conjunction);
            this.conjunctionPlans = new ConcurrentHashMap<>();
            this.registriesForSubConjunctions = new ConcurrentHashMap<>();
        }

        @Override
        protected Actor.Driver<ConjunctionNode> createNode(ConceptMap bounds) {
            // TODO: Move to pre-initialisation
            ConjunctionController.ConjunctionStreamPlan conjunctionStreamPlan = conjunctionPlans.computeIfAbsent(bounds.concepts().keySet(), identifiers -> {
                Set<Variable> boundVars = Iterators.iterate(identifiers).map(id -> key.pattern().variable(id)).toSet();
                ReasonerPlanner.Plan plan = NodeRegistry.this.planner.getPlan(key, boundVars);
                ConjunctionController.ConjunctionStreamPlan csPlan = ConjunctionController.ConjunctionStreamPlan.create(plan.plan(), identifiers, key.pattern().retrieves());
                createSubregistries(csPlan);
                return csPlan;
            });
            return registriesForSubConjunctions.get(conjunctionStreamPlan).createNode(bounds);
        }

        private void createSubregistries(ConjunctionController.ConjunctionStreamPlan rootPlan) {
            Stack<ConjunctionController.ConjunctionStreamPlan> remainingPlans = new Stack<>();
            rootPlan.pu
            remainingPlans.add(rootPlan);
            while (!remainingPlans.isEmpty()) {
                ConjunctionController.ConjunctionStreamPlan nextPlan = remainingPlans.pop();
                if (nextPlan.isCompoundStreamPlan()) {
                    registriesForSubConjunctions.
                    for (int i=0; i< nextPlan.asCompoundStreamPlan().size(); i++) {

                    }
                }
            }
        }



        public class SubConjunctionRegistry extends SubRegistry<ConjunctionController.ConjunctionStreamPlan, ConjunctionNode> {

            private final ResolvableConjunction conjunction;

            private SubConjunctionRegistry(ResolvableConjunction conjunction, ConjunctionController.ConjunctionStreamPlan conjunctionStreamPlan) {
                super(conjunctionStreamPlan);
                this.conjunction = conjunction;
            }

            @Override
            protected Actor.Driver<ConjunctionNode> createNode(ConceptMap bounds) {
                return createDriver(driver -> new ConjunctionNode(conjunction, bounds, key, NodeRegistry.this, driver));
            }
        }
    }
}
