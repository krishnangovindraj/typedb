package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.common.collection.Collections;
import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController.ConjunctionStreamPlan.CompoundStreamPlan;
import com.vaticle.typedb.core.reasoner.planner.ConjunctionGraph;
import com.vaticle.typedb.core.reasoner.planner.ReasonerPlanner;
import com.vaticle.typedb.core.reasoner.planner.RecursivePlanner;
import com.vaticle.typedb.core.reasoner.v4.nodes.ConclusionNode;
import com.vaticle.typedb.core.reasoner.v4.nodes.ConjunctionNode;
import com.vaticle.typedb.core.reasoner.v4.nodes.MaterialiserNode;
import com.vaticle.typedb.core.reasoner.v4.nodes.ResolvableNode;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

// TODO: See if we can use ConjunctionStreamPlan as the only one key we need. The nodes can do safe-downcasting
public class NodeRegistry {
    private final ActorExecutorGroup executorService;
    private final Map<CompoundStreamPlan, SubConjunctionRegistry> conjunctionSubRegistries;
    private final Map<Rule.Conclusion, ConclusionRegistry> conclusionSubRegistries;
    private final Map<Retrievable, RetrievableRegistry> retrievableSubRegistries;
    private final Map<Concludable, ConcludableRegistry> concludableSubRegistries;
    private final Map<Concludable, ConcludableRegistry> negatedSubRegistries;
    private final Map<ReasonerPlanner.CallMode, ConjunctionController.ConjunctionStreamPlan> csPlans;
    private final Set<ActorNode<?>> roots;
    private final LogicManager logicManager;
    private final RecursivePlanner planner;
    private AtomicBoolean terminated;
    private final TraversalEngine traversalEngine;
    private final ConceptManager conceptManager;
    private Actor.Driver<MaterialiserNode> materialiserNode;

    public NodeRegistry(ActorExecutorGroup executorService,
                        ConceptManager conceptManager, LogicManager logicManager, TraversalEngine traversalEngine,
                        ReasonerPlanner planner) {
        this.executorService = executorService;
        this.traversalEngine = traversalEngine;
        this.conceptManager = conceptManager;
        this.logicManager = logicManager;
        this.planner = planner.asRecursivePlanner();
        this.csPlans = new ConcurrentHashMap<>();
        this.conjunctionSubRegistries = new ConcurrentHashMap<>();
        this.conclusionSubRegistries = new ConcurrentHashMap<>();
        this.retrievableSubRegistries = new ConcurrentHashMap<>();
        this.concludableSubRegistries = new ConcurrentHashMap<>();
        this.negatedSubRegistries = new ConcurrentHashMap<>();
        this.roots = new ConcurrentSet<>();
        this.terminated = new AtomicBoolean(false);
    }

    void prepare(ResolvableConjunction rootConjunction, ConceptMap bounds) {
        Set<Variable> boundVars = Iterators.iterate(bounds.concepts().keySet()).map(id -> rootConjunction.pattern().variable(id)).toSet();
        planner.plan(rootConjunction, boundVars);
        cacheConjunctionStreamPlans(new ReasonerPlanner.CallMode(rootConjunction, boundVars));
        csPlans.forEach((callMode, csPlan) -> {
            if (csPlan.isCompoundStreamPlan()) {
                populateConjunctionRegistries(callMode.conjunction, csPlan.asCompoundStreamPlan());
            }
        });
        Iterators.iterate(csPlans.keySet()).map(callMode -> callMode.conjunction).distinct()
                .forEachRemaining(this::populateResolvableRegistries);

        Iterators.iterate(concludableSubRegistries.keySet())
                .flatMap(concludable -> Iterators.iterate(logicManager.applicableRules(concludable).keySet()))
                .forEachRemaining(this::registerConclusions);
        materialiserNode = Actor.driver(
                materialiserNodeDriver -> new MaterialiserNode(this, materialiserNodeDriver),
                executorService);
    }

    private void registerConclusions(Rule rule) {
        if (!this.conclusionSubRegistries.containsKey(rule.conclusion())) {
            conclusionSubRegistries.put(rule.conclusion(), new ConclusionRegistry(rule.conclusion()));
        }
    }

    private void cacheConjunctionStreamPlans(ReasonerPlanner.CallMode callMode) {
        if (!csPlans.containsKey(callMode)) {
            ReasonerPlanner.Plan plan = planner.getPlan(callMode.conjunction, callMode.mode);
            Set<Identifier.Variable.Retrievable> modeIds = Iterators.iterate(callMode.mode).map(Variable::id)
                    .filter(Identifier::isRetrievable).map(Identifier.Variable::asRetrievable).toSet();
            ConjunctionController.ConjunctionStreamPlan csPlan = ConjunctionController.ConjunctionStreamPlan.createUnflattened(
                    plan.plan(), modeIds, callMode.conjunction.pattern().retrieves());
            csPlans.put(callMode, csPlan);

            Set<Variable> runningBounds = new HashSet<>(callMode.mode);
            for (Resolvable<?> resolvable : plan.plan()) {
                if (resolvable.isConcludable()) {
                    Set<Variable> concludableBounds = Collections.intersection(runningBounds, resolvable.variables());
                    planner.triggeredCalls(resolvable.asConcludable(), concludableBounds, null)
                            .forEach(this::cacheConjunctionStreamPlans);
                }
                runningBounds.addAll(resolvable.variables());
            }
        }
    }

    private void populateConjunctionRegistries(ResolvableConjunction conjunction, CompoundStreamPlan compoundStreamPlan) {
        conjunctionSubRegistries.put(compoundStreamPlan, new SubConjunctionRegistry(conjunction, compoundStreamPlan));
    }

    private void populateResolvableRegistries(ResolvableConjunction conjunction) {
        logicManager().compile(conjunction).forEach(resolvable -> {
            if (resolvable.isConcludable()) {
                ConjunctionGraph.ConjunctionNode infoNode = planner.conjunctionGraph().conjunctionNode(conjunction);
                concludableSubRegistries.computeIfAbsent(resolvable.asConcludable(), concludable -> new ConcludableRegistry(concludable, infoNode));
            } else if (resolvable.isRetrievable()) {
                retrievableSubRegistries.computeIfAbsent(resolvable.asRetrievable(), RetrievableRegistry::new);
            } else if (resolvable.isNegated()) {
                throw TypeDBException.of(UNIMPLEMENTED);
            } else throw TypeDBException.of(ILLEGAL_STATE);
        });
    }

    public ConjunctionController.ConjunctionStreamPlan conjunctionStreamPlan(ResolvableConjunction conjunction, ConceptMap bounds) {
        ReasonerPlanner.CallMode callMode = new ReasonerPlanner.CallMode(conjunction,
                Iterators.iterate(bounds.concepts().keySet()).map(id -> conjunction.pattern().variable(id)).toSet());
        return csPlans.get(callMode);
    }

    public Actor.Driver<MaterialiserNode> materialiserNode() {
        return materialiserNode;
    }

    public SubConjunctionRegistry conjunctionSubRegistry(CompoundStreamPlan compoundStreamPlan) {
        assert conjunctionSubRegistries.containsKey(compoundStreamPlan);
        return conjunctionSubRegistries.get(compoundStreamPlan);
    }

    public RetrievableRegistry retrievableSubRegistry(Retrievable retrievable) {
        assert retrievableSubRegistries.containsKey(retrievable);
        return retrievableSubRegistries.get(retrievable);
    }

    public ConcludableRegistry concludableSubRegistry(Concludable concludable) {
        assert concludableSubRegistries.containsKey(concludable);
        return concludableSubRegistries.get(concludable);
    }

    public ConcludableRegistry negatedSubRegistry(Negated negated) {
        throw TypeDBException.of(UNIMPLEMENTED);
    }

    public <NODE extends ActorNode<NODE>> Actor.Driver<NODE> createLocalNode(Function<Actor.Driver<NODE>, NODE> actorFn) {
        return createDriverAndInitialise(actorFn);
    }

    public <NODE extends ActorNode<NODE>> NODE createRoot(Function<Actor.Driver<NODE>, NODE> actorFn) {
        Actor.Driver<NODE> driver = createDriverAndInitialise(actorFn);
        this.roots.add(driver.actor());
        return driver.actor();
    }

    private <NODE extends ActorNode<NODE>> Actor.Driver<NODE> createDriverAndInitialise(Function<Actor.Driver<NODE>, NODE> actorFn) {
        Actor.Driver<NODE> nodeDriver = Actor.driver(actorFn, executorService);
        nodeDriver.execute(node -> node.initialise());
        return nodeDriver;
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

    public SubRegistry<?, ?> resolvableSubRegistry(Resolvable<?> resolvable) {
        if (resolvable.isConcludable()) return concludableSubRegistry(resolvable.asConcludable());
        else if (resolvable.isRetrievable()) return retrievableSubRegistry(resolvable.asRetrievable());
        else if (resolvable.isNegated()) return negatedSubRegistry(resolvable.asNegated());
        else throw TypeDBException.of(ILLEGAL_STATE);
    }


    public SubRegistry<Rule.Conclusion, ConclusionNode> conclusionSubRegistry(Rule.Conclusion conclusion) {
        assert conclusionSubRegistries.containsKey(conclusion);
        return conclusionSubRegistries.get(conclusion);
    }


    public SubRegistry<?, ?> getRegistry(ConjunctionController.ConjunctionStreamPlan csPlan) {
        return csPlan.isCompoundStreamPlan() ?
                conjunctionSubRegistry(csPlan.asCompoundStreamPlan()) :
                resolvableSubRegistry(csPlan.asResolvablePlan().resolvable());
    }

    public RecursivePlanner planner() {
        return planner;
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

    public class RetrievableRegistry extends SubRegistry<Retrievable, ResolvableNode.RetrievableNode> {

        private RetrievableRegistry(Retrievable retrievable) {
            super(retrievable);
        }

        @Override
        protected Actor.Driver<ResolvableNode.RetrievableNode> createNode(ConceptMap bounds) {
            return createDriverAndInitialise(driver -> new ResolvableNode.RetrievableNode(key, bounds, NodeRegistry.this, driver));
        }
    }

    public class ConcludableRegistry extends SubRegistry<Concludable, ResolvableNode.ConcludableNode> {


        private final ConjunctionGraph.ConjunctionNode infoNode;

        private ConcludableRegistry(Concludable concludable, ConjunctionGraph.ConjunctionNode infoNode) {
            super(concludable);
            this.infoNode = infoNode;
        }

        @Override
        protected Actor.Driver<ResolvableNode.ConcludableNode> createNode(ConceptMap bounds) {
            return createDriverAndInitialise(driver -> new ResolvableNode.ConcludableNode(key, bounds, infoNode, NodeRegistry.this, driver));
        }

    }

    public class SubConjunctionRegistry extends SubRegistry<ConjunctionController.ConjunctionStreamPlan.CompoundStreamPlan, ConjunctionNode> {

        private final ResolvableConjunction conjunction;

        private SubConjunctionRegistry(ResolvableConjunction conjunction, ConjunctionController.ConjunctionStreamPlan.CompoundStreamPlan conjunctionStreamPlan) {
            super(conjunctionStreamPlan);
            this.conjunction = conjunction;
        }

        @Override
        protected Actor.Driver<ConjunctionNode> createNode(ConceptMap bounds) {
            return createDriverAndInitialise(driver -> new ConjunctionNode(conjunction, bounds, key, NodeRegistry.this, driver));
        }
    }

    public class ConclusionRegistry extends SubRegistry<Rule.Conclusion, ConclusionNode> {


        private ConclusionRegistry(Rule.Conclusion conclusion) {
            super(conclusion);
        }

        @Override
        protected Actor.Driver<ConclusionNode> createNode(ConceptMap bounds) {
            return createDriverAndInitialise(driver -> new ConclusionNode(key, bounds, NodeRegistry.this, driver));
        }

    }
}
