package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.common.collection.Collections;
import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.perfcounter.PerfCounters;
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
import com.vaticle.typedb.core.reasoner.common.ReasonerPerfCounters;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController.ConjunctionStreamPlan.CompoundStreamPlan;
import com.vaticle.typedb.core.reasoner.planner.ConjunctionGraph;
import com.vaticle.typedb.core.reasoner.planner.ReasonerPlanner;
import com.vaticle.typedb.core.reasoner.planner.RecursivePlanner;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Modifiers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

// TODO: See if we can use ConjunctionStreamPlan as the only one key we need. The nodes can do safe-downcasting
public class NodeRegistry {
    private final ActorExecutorGroup executorService;
    private final PerfCounterFields perfCountersFields;
    private final Map<CompoundStreamPlan, SubConjunctionRegistry> conjunctionSubRegistries;
    private final Map<Rule.Conclusion, ConclusionRegistry> conclusionSubRegistries;
    private final Map<Retrievable, RetrievableRegistry> retrievableSubRegistries;
    private final Map<Concludable, ConcludableRegistry> concludableSubRegistries;
    private final Map<Negated, NegatedRegistry> negatedSubRegistries;
    private final Map<ReasonerPlanner.CallMode, ConjunctionController.ConjunctionStreamPlan> csPlans;
    private final Set<ActorNode<?>> roots;
    private final LogicManager logicManager;
    private final RecursivePlanner planner;
    ;
    private final ReasonerPerfCounters perfCounters;
    private AtomicBoolean terminated;
    private final TraversalEngine traversalEngine;
    private final ConceptManager conceptManager;
    private Actor.Driver<MaterialiserNode> materialiserNode;

    private final AtomicInteger nodeAgeClock;
    private final Set<ConjunctionController.ConjunctionStreamPlan> cyclicConjunctionStreamPlans;

    public NodeRegistry(ActorExecutorGroup executorService, ReasonerPerfCounters perfCounters,
                        ConceptManager conceptManager, LogicManager logicManager, TraversalEngine traversalEngine,
                        ReasonerPlanner planner) {
        this.executorService = executorService;
        this.perfCounters = perfCounters;
        this.perfCountersFields = new PerfCounterFields(perfCounters);
        this.traversalEngine = traversalEngine;
        this.conceptManager = conceptManager;
        this.logicManager = logicManager;
        this.planner = planner.asRecursivePlanner();
        this.csPlans = new HashMap<>();
        this.cyclicConjunctionStreamPlans = new HashSet<>();
        this.conjunctionSubRegistries = new HashMap<>();
        this.conclusionSubRegistries = new HashMap<>();
        this.retrievableSubRegistries = new HashMap<>();
        this.concludableSubRegistries = new HashMap<>();
        this.negatedSubRegistries = new HashMap<>();
        this.roots = new ConcurrentSet<>();
        this.terminated = new AtomicBoolean(false);
        this.nodeAgeClock = new AtomicInteger();
    }

    public Integer nextNodeAge() {
        return nodeAgeClock.getAndIncrement();
    }

    public void prepare(ResolvableConjunction rootConjunction, ConceptMap rootBounds, Modifiers.Filter rootFilter) {
        Set<Variable> boundVars = iterate(rootBounds.concepts().keySet()).map(id -> rootConjunction.pattern().variable(id)).toSet();
        planner.plan(rootConjunction, boundVars);
        cacheConjunctionStreamPlans(new ReasonerPlanner.CallMode(rootConjunction, boundVars), rootFilter.variables());
//        csPlans.forEach((callMode, csPlan) -> {
//            cacheIsCyclicConjunctionStreamPlans(planner.conjunctionGraph().conjunctionNode(callMode.conjunction), csPlan);
//        });
        csPlans.forEach((callMode, csPlan) -> {
            if (csPlan.isCompoundStreamPlan()) {
                populateConjunctionRegistries(callMode.conjunction, csPlan.asCompoundStreamPlan());
            }
        });
        iterate(csPlans.keySet()).map(callMode -> callMode.conjunction).distinct()
                .forEachRemaining(this::populateResolvableRegistries);

        iterate(concludableSubRegistries.keySet())
                .flatMap(concludable -> iterate(logicManager.applicableRules(concludable).keySet()))
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

    private void cacheConjunctionStreamPlans(ReasonerPlanner.CallMode callMode, Set<Identifier.Variable.Retrievable> outputVariables) {
        if (!csPlans.containsKey(callMode)) {
            ReasonerPlanner.Plan plan = planner.getPlan(callMode.conjunction, callMode.mode);
            Set<Identifier.Variable.Retrievable> modeIds = iterate(callMode.mode).map(Variable::id)
                    .filter(Identifier::isRetrievable).map(Identifier.Variable::asRetrievable).toSet();
            ConjunctionController.ConjunctionStreamPlan csPlan = ConjunctionController.ConjunctionStreamPlan.createUnflattened(
                    plan.plan(), modeIds, outputVariables);
            csPlans.put(callMode, csPlan);

            Set<Variable> runningBounds = new HashSet<>(callMode.mode);
            for (Resolvable<?> resolvable : plan.plan()) {
                if (resolvable.isConcludable()) {
                    Set<Variable> concludableBounds = Collections.intersection(runningBounds, resolvable.variables());
                    // Ooops, I need the rules too.
                    Map<ResolvableConjunction, Set<Identifier.Variable.Retrievable>> conclusionVars = new HashMap<>();
                    logicManager.applicableRules(resolvable.asConcludable()).keySet().forEach(rule -> {
                        iterate(rule.condition().disjunction().conjunctions())
                                .forEachRemaining(conjunction -> conclusionVars.put(conjunction, rule.conclusion().retrievableIds()));
                    });
                    planner.triggeredCalls(resolvable.asConcludable(), concludableBounds, null)
                            .forEach(triggeredMode -> cacheConjunctionStreamPlans(triggeredMode, conclusionVars.get(triggeredMode.conjunction)));
                } else if (resolvable.isNegated()) {
                    Set<Variable> negatedBounds = Collections.intersection(runningBounds, resolvable.variables());
                    Set<Identifier.Variable.Retrievable> negatedBoundIds = iterate(negatedBounds).map(v -> v.id().asRetrievable()).toSet();
                    resolvable.asNegated().disjunction().conjunctions().forEach(nestedConj -> {
                        cacheConjunctionStreamPlans(new ReasonerPlanner.CallMode(nestedConj, negatedBounds), negatedBoundIds);
                    });
                }
                iterate(resolvable.variables()).filter(v -> v.id().isRetrievable()).forEachRemaining(runningBounds::add);
            }
        }
    }
//
//    private boolean cacheIsCyclicConjunctionStreamPlans(ConjunctionGraph.ConjunctionNode infoNode, ConjunctionController.ConjunctionStreamPlan conjunctionStreamPlan) {
//        boolean isCyclic = false;
//        if (conjunctionStreamPlan.isResolvablePlan()) {
//            isCyclic  = infoNode.cyclicConcludables().contains(conjunctionStreamPlan.asResolvablePlan().resolvable());
//        } else if (conjunctionStreamPlan.isCompoundStreamPlan()) {
//            for (int i=0; i < conjunctionStreamPlan.asCompoundStreamPlan().size(); i++) {
//                ConjunctionController.ConjunctionStreamPlan child = conjunctionStreamPlan.asCompoundStreamPlan().childAt(i);
//                isCyclic = isCyclic || cacheIsCyclicConjunctionStreamPlans(infoNode, child);
//            }
//        } else throw TypeDBException.of(ILLEGAL_STATE);
//
//        if (isCyclic) {
//            cyclicConjunctionStreamPlans.add(conjunctionStreamPlan);
//            return true;
//        } else return false;
//    }

    private void populateConjunctionRegistries(ResolvableConjunction conjunction, CompoundStreamPlan compoundStreamPlan) {
        conjunctionSubRegistries.put(compoundStreamPlan, new SubConjunctionRegistry(conjunction, compoundStreamPlan));
        for (int i = 0; i < compoundStreamPlan.size(); i++) {
            if (compoundStreamPlan.childAt(i).isCompoundStreamPlan()) {
                populateConjunctionRegistries(conjunction, compoundStreamPlan.childAt(i).asCompoundStreamPlan());
            }
        }
    }

    private void populateResolvableRegistries(ResolvableConjunction conjunction) {
        logicManager().compile(conjunction).forEach(resolvable -> {
            if (resolvable.isConcludable()) {
                ConjunctionGraph.ConjunctionNode infoNode = planner.conjunctionGraph().conjunctionNode(conjunction);
                concludableSubRegistries.computeIfAbsent(resolvable.asConcludable(), concludable -> new ConcludableRegistry(concludable, infoNode));
            } else if (resolvable.isRetrievable()) {
                retrievableSubRegistries.computeIfAbsent(resolvable.asRetrievable(), RetrievableRegistry::new);
            } else if (resolvable.isNegated()) {
                negatedSubRegistries.computeIfAbsent(resolvable.asNegated(), NegatedRegistry::new);
            } else throw TypeDBException.of(ILLEGAL_STATE);
        });
    }

    public ConjunctionController.ConjunctionStreamPlan conjunctionStreamPlan(ResolvableConjunction conjunction, ConceptMap bounds) {
        ReasonerPlanner.CallMode callMode = new ReasonerPlanner.CallMode(conjunction,
                iterate(bounds.concepts().keySet()).map(id -> conjunction.pattern().variable(id)).toSet());
        return csPlans.get(callMode);
    }

    public Actor.Driver<MaterialiserNode> materialiserNode() {
        return materialiserNode;
    }

    // Now that we have conclusions, Top-level conjunction nodes are local too. But the ones after them aren't
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

    public NegatedRegistry negatedSubRegistry(Negated negated) {
        assert negatedSubRegistries.containsKey(negated);
        return negatedSubRegistries.get(negated);
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

    public ReasonerPerfCounters perfCounters() {
        return perfCounters;
    }

    public PerfCounterFields perfCounterFields() {
        return perfCountersFields;
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

    public class ConcludableRegistry extends SubRegistry<Concludable, ConcludableNode> {


        private final ConjunctionGraph.ConjunctionNode infoNode; // TODO: Remove

        private ConcludableRegistry(Concludable concludable, ConjunctionGraph.ConjunctionNode infoNode) {
            super(concludable);
            this.infoNode = infoNode;
        }

        @Override
        protected Actor.Driver<ConcludableNode> createNode(ConceptMap bounds) {
            return createDriverAndInitialise(driver -> new ConcludableNode(key, bounds, NodeRegistry.this, driver));
        }

    }

    public class NegatedRegistry extends SubRegistry<Negated, NegatedNode> {

        private NegatedRegistry(Negated negated) {
            super(negated);
        }

        @Override
        protected Actor.Driver<NegatedNode> createNode(ConceptMap bounds) {
            return createDriverAndInitialise(driver -> new NegatedNode(key, bounds, NodeRegistry.this, driver));
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

    public class PerfCounterFields {
        public final PerfCounters.Counter subConjunctionNodes;
        public final PerfCounters.Counter resolvableNodes;
        public final PerfCounters.Counter materialisations;
        public final PerfCounters.Counter answersInTables;

        private PerfCounterFields(PerfCounters perfCounters) {

            subConjunctionNodes = perfCounters.register("v4_subConjunctionNodes");
            ;
            resolvableNodes = perfCounters.register("v4_resolvableNodes");
            materialisations = perfCounters.register("v4_materialisations");
            answersInTables = perfCounters.register("v4_tabledAnswers");
        }

    }
}
