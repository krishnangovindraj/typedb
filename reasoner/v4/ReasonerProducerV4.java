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

package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.producer.Producer;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.*;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.ExplainablesManager;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.reasoner.answer.PartialExplanation;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController;
import com.vaticle.typedb.core.reasoner.planner.ReasonerPlanner;
import com.vaticle.typedb.core.reasoner.v4.nodes.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.nodes.NodeRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.reasoner.v4.ReasonerProducerV4.State.EXCEPTION;
import static com.vaticle.typedb.core.reasoner.v4.ReasonerProducerV4.State.FINISHED;
import static com.vaticle.typedb.core.reasoner.v4.ReasonerProducerV4.State.INIT;
import static com.vaticle.typedb.core.reasoner.v4.ReasonerProducerV4.State.INITIALISING;
import static com.vaticle.typedb.core.reasoner.v4.ReasonerProducerV4.State.PULLING;
import static com.vaticle.typedb.core.reasoner.v4.ReasonerProducerV4.State.READY;


@ThreadSafe
public abstract class ReasonerProducerV4<ROOTNODE extends ActorNode<ROOTNODE>, ANSWER> implements Producer<ANSWER>, ReasonerConsumerV4 {

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerProducerV4.class);

    protected final NodeRegistry nodeRegistry;
    protected final ExplainablesManager explainablesManager;
    final AtomicInteger requiredAnswers;
    final Options.Query options;
    private Throwable exception;
    Queue<ANSWER> queue;
    State state;
    protected ROOTNODE rootNode; // TODO: Make final, init in constructor, change return type of initialiseRoot
    final Set<ANSWER> seenAnswers;

    enum State {
        INIT,
        INITIALISING,
        READY,
        PULLING,
        FINISHED,
        EXCEPTION
    }

    // TODO: this class should not be a Producer, it implements a different async processing mechanism
    private ReasonerProducerV4(Options.Query options, NodeRegistry nodeRegistry, ExplainablesManager explainablesManager) {
        this.options = options;
        this.nodeRegistry = nodeRegistry;
        this.explainablesManager = explainablesManager;
        this.queue = null;
        this.requiredAnswers = new AtomicInteger();
        this.state = INIT;
        seenAnswers = new HashSet<>();
    }

    NodeRegistry nodeRegistry() {
        return nodeRegistry;
    }

    @Override
    public synchronized void produce(Queue<ANSWER> queue, int requestedAnswers, Executor executor) {
        assert this.queue == null || this.queue == queue;
        assert requestedAnswers > 0;
        if (state == EXCEPTION) queue.done(exception);
        else if (state == FINISHED) queue.done();
        else {
            this.queue = queue;
            requiredAnswers.addAndGet(requestedAnswers);
            if (state == INIT) initialise();
            else if (state == READY) pull();
        }
    }

    private void initialise() {
        assert state == INIT;
        state = INITIALISING;
        prepare();
        rootNode = createRootNode();
        state = READY;
        pull();
    }

    protected abstract void prepare();

    abstract ROOTNODE createRootNode();

    synchronized void pull() {
        assert state == READY;
        state = PULLING;
        readNextAnswer();
    }

    abstract void readNextAnswer();

    @Override
    public synchronized void finish() {
        // note: root resolver calls this single-threaded, so is thread safe
        LOG.trace("All answers found.");
        if (state != FINISHED && state != EXCEPTION) {
            if (queue == null) {
                assert state != PULLING;
                assert requiredAnswers.get() == 0;
            } else {
                requiredAnswers.set(0);
                queue.done();
            }
        }
    }

    @Override
    public synchronized void exception(Throwable e) {
        LOG.error("ReasonerProducer exception called with exception: ", e);
        if (state != FINISHED && state != EXCEPTION) {
            exception = e;
            if (queue == null) {
                assert state != PULLING;
                assert requiredAnswers.get() == 0;
            } else {
                requiredAnswers.set(0);
                queue.done(e.getCause());
            }
        }
    }

    @Override
    public void recycle() {

    }

    public static class Basic extends ReasonerProducerV4<Basic.RootNode, ConceptMap> {

        private final ResolvableDisjunction disjunction;
        private final Modifiers.Filter filter;
        private AtomicInteger answersReceived;
        private final Map<ActorNode.Port, ResolvableConjunction> portToConjunction;

        public Basic(ResolvableDisjunction disjunction, Modifiers.Filter filter, Options.Query options, NodeRegistry nodeRegistry, ExplainablesManager explainablesManager) {
            super(options, nodeRegistry, explainablesManager);
            this.disjunction = disjunction;
            this.filter = filter;
            this.answersReceived = new AtomicInteger(0);
            this.portToConjunction = new HashMap<>();
        }

        @Override
        protected void prepare() {
            nodeRegistry.prepare(disjunction, ConceptMap.EMPTY, filter);
        }

        @Override
        RootNode createRootNode() {
            return nodeRegistry.createRoot(nodeDriver -> new RootNode(nodeRegistry, nodeDriver));
        }

        void readNextAnswer() {
            int nextAnswerIndex = answersReceived.getAndIncrement();
            rootNode.driver().execute(rootNode -> rootNode.readAnswerAt(null, new Request.ReadAnswer(nextAnswerIndex)));
        }

        @Override
        public synchronized void receiveAnswer(ConceptMap answer) {
            state = READY;
            if (!seenAnswers.contains(answer)) {
                explainablesManager.setAndRecordExplainables(answer);
                seenAnswers.add(answer);
                queue.put(answer);
                if (requiredAnswers.decrementAndGet() > 0) pull();
            } else {
                if (requiredAnswers.get() > 0) pull();
            }
        }

        class RootNode extends ActorNode<RootNode> {

            protected RootNode(NodeRegistry nodeRegistry, Driver<RootNode> driver) {
                super(nodeRegistry, driver, () -> "RootNode: " + disjunction.pattern());
            }

            @Override
            public void initialise() {
                disjunction.conjunctions().forEach(conjunction -> {
                    ConjunctionController.ConjunctionStreamPlan csPlan = nodeRegistry.conjunctionStreamPlan(conjunction, ConceptMap.EMPTY);
                    NodeRegistry.SubRegistry<?, ?> subRegistry = nodeRegistry.getRegistry(csPlan);
                    Port port = createPort(subRegistry.getNode(ConceptMap.EMPTY));
                    ports.add(port);
                    activePorts.add(port);
                    portToConjunction.put(port, conjunction);
                });
                nodeRegistry.perfCounters().startPeriodicPrinting();
            }

            @Override
            public void terminate(Throwable e) {
                nodeRegistry.perfCounters().stopPrinting();
                super.terminate(e);
                Basic.this.exception(e);
            }

            @Override
            protected void readAnswerAt(Port _ignored, Request.ReadAnswer readAnswer) {
//                assert readAnswer.index == ???;
//                computeNextAnswer(port, readAnswer.index);
                boolean pulledOnOne = false;
                for (Port port: ports) {
                    if (port.isReady()) {
                        port.readNext();
                        pulledOnOne = true; // KGFLAG: Strategy
                    }
                }
//                assert pulledOnOne;
            }

            @Override
            protected void computeNextAnswer(Port reader, int index) {
                // port.readNext();
                assert false;
            }


            @Override
            protected void handleAnswer(Port onPort, Response.Answer answer) {
                ResolvableConjunction conj = portToConjunction.get(onPort);
                Basic.this.receiveAnswer(transformAnswer(conj, answer.answer()));
            }

            private ConceptMap transformAnswer(ResolvableConjunction conj, ConceptMap answer) {
                if (options.explain()) {
                    ReasonerPlanner.Plan plan = nodeRegistry.planner().getPlan(conj, Collections.emptySet());
                    ConceptMap withExplainables = answer;
                    Set<Identifier.Variable.Retrievable> bounds = new HashSet<>();
                    for (Resolvable<?> resolvable : plan.plan()) {
                        if (resolvable.isConcludable()) {
                            Concludable concludable = resolvable.asConcludable();
                            if (concludable.isRelation()) {
                                withExplainables = withExplainables.withExplainableConcept(concludable.asRelation().generatingVariable().id(), concludable.pattern());
                            } else if (concludable.isAttribute()) {
                                withExplainables = withExplainables.withExplainableConcept(concludable.asAttribute().generatingVariable().id(), concludable.pattern());
                            } else if (concludable.isIsa()) {
                                // TODO?
                                // withExplainables = withExplainables.withExplainableConcept(concludable.asIsa().generatingVariable().id(), concludable.pattern());
                            } else if (concludable.isHas()) {
                                withExplainables = withExplainables.withExplainableOwnership(concludable.asHas().owner().id(), concludable.asHas().attribute().id(), concludable.pattern());
                            } else {
                                throw TypeDBException.of(ILLEGAL_STATE);
                            }
                            explainablesManager.recordBounds(concludable.pattern(), new HashSet<>(bounds));
                        }
                        bounds.addAll(resolvable.retrieves());
                    }
                    return withExplainables;
                } else {
                    return answer.filter(filter);
                }
            }

            @Override
            protected void handleDone(Port onPort) {
                activePorts.remove(onPort);
                if (activePorts.isEmpty()) {
                    Basic.this.finish();
                    nodeRegistry.perfCounters().stopPrinting();
                }
            }
        }
    }

    // This requires some work to track the bounds for efficient re-use
    public static class Explain extends ReasonerProducerV4<Explain.ExplainNode, Explanation> {
        private final Concludable concludable;
        private final ConceptMap bounds;
        private AtomicInteger answersReceived;

        public Explain(Concludable explainableConcludable, ConceptMap explainableBounds, Options.Query options, NodeRegistry nodeRegistry, ExplainablesManager explainablesManager) {
            super(options, nodeRegistry, explainablesManager);
            this.concludable = explainableConcludable;
            this.bounds = explainableBounds;
            this.answersReceived = new AtomicInteger(0);
        }

        @Override
        protected void prepare() {
            // The node registry is already prepared
        }

        @Override
        ExplainNode createRootNode() {
            return nodeRegistry.createRoot(nodeDriver -> new ExplainNode(nodeRegistry, nodeDriver));
        }

        @Override
        void readNextAnswer() {
            int nextAnswerIndex = answersReceived.getAndIncrement();
            rootNode.driver().execute(rootNode -> rootNode.readAnswerAt(null, new Request.ReadAnswer(nextAnswerIndex)));
        }

        public synchronized void receiveAnswer(Explanation answer) {
            state = READY;
            if (!seenAnswers.contains(answer) ) {
                seenAnswers.add(answer);
                queue.put(answer);
                if (requiredAnswers.decrementAndGet() > 0) pull();
            } else {
                if (requiredAnswers.get() > 0) pull();
            }
        }

        @Override
        public void receiveAnswer(ConceptMap conceptMap) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        class ExplainNode extends ActorNode<ExplainNode> {
            // We try to plug straight into the conditions of the rules which the concludable unified with.
            // This also means re-using the original bounds of the concludable, and filtering out the answers that don't match.
            private final Map<Port, Pair<Unifier, Unifier.Requirements.Instance>> portUnifiers; // TODO: Improve
            private final Map<Port, Rule.Condition.ConditionBranch> portToRuleCondition;

            protected ExplainNode(NodeRegistry nodeRegistry, Driver<ExplainNode> driver) {
                super(nodeRegistry, driver, () -> "ExplainNode: " + concludable.pattern() + "/" + bounds);
                portUnifiers = new HashMap<>();
                portToRuleCondition = new HashMap<>();
            }

            @Override
            public void initialise() {
                // TODO: This seems a bit inefficient. We use multiple ports for the same node just because we have multiple unifiers
                Set<Identifier.Variable.Retrievable> reusableBoundVariables = explainablesManager.getBounds(concludable.pattern());
                ConceptMap reusableBounds = bounds.filter(reusableBoundVariables);
                nodeRegistry.logicManager().applicableRules(concludable).forEach((rule, unifiers) -> {
                    unifiers.forEach(unifier -> unifier.unify(reusableBounds).ifPresent(boundsAndRequirements -> {
                        rule.condition().branches().forEach(branch -> {
                            ConceptMap filteredBounds = boundsAndRequirements.first().filter(branch.conjunction().pattern().retrieves());
                            ConjunctionController.ConjunctionStreamPlan csPlan = nodeRegistry.conjunctionStreamPlan(branch.conjunction(), filteredBounds);
                            Port port = createPort(nodeRegistry.getRegistry(csPlan).getNode(filteredBounds));
                            portUnifiers.put(port, new Pair<>(unifier, boundsAndRequirements.second()));
                            portToRuleCondition.put(port, branch);
                        });
                    }));
                });
                nodeRegistry.perfCounters().startPeriodicPrinting();
            }

            @Override
            public void terminate(Throwable e) {
                nodeRegistry.perfCounters().stopPrinting();
                super.terminate(e);
                Explain.this.exception(e);
            }

            @Override
            protected void readAnswerAt(Port _ignored, Request.ReadAnswer readAnswer) {
                // TODO: Improve based on Basic.RootNode
                for (Port port: ports) {
                    if (port.isReady()) {
                        port.readNext();
                    }
                }
            }

            @Override
            protected void computeNextAnswer(Port reader, int index) {
                // port.readNext();
                assert false;
            }

            @Override
            protected void handleAnswer(Port onPort, Response.Answer answer) {
                // Check if it matches the bounds
                Pair<Unifier, Unifier.Requirements.Instance> unifiers = portUnifiers.get(onPort);
                Map<Identifier.Variable, Concept> toUnunify = new HashMap<>(answer.answer().concepts());
                boolean anyMatch = unifiers.first().unUnify(toUnunify, unifiers.second())
                        .anyMatch(conceptMap -> conceptMap.equals(bounds));
                if (anyMatch) {
                    Explain.this.receiveAnswer(transformAnswer(onPort, answer.answer()));
                } else {
                    readNextAnswer();
                }
            }

            private Explanation transformAnswer(Port onPort, ConceptMap answer) {
                Rule.Condition.ConditionBranch branch = portToRuleCondition.get(onPort);
                Pair<Unifier, Unifier.Requirements.Instance> unifiers = portUnifiers.get(onPort);
                Map<Identifier.Variable, Concept> conclusionConceptMap = new HashMap<>(answer.concepts());
                PartialExplanation pe = PartialExplanation.create(
                        branch.rule(),
                        conclusionConceptMap,
                        answer
                );
                return new Explanation(branch.rule(), unifiers.first().mapping(), pe.conclusionAnswer(), pe.conditionAnswer());
            }

            @Override
            protected void handleDone(Port onPort) {
                activePorts.remove(onPort);
                if (activePorts.isEmpty()) {
                    Explain.this.finish();
                    nodeRegistry.perfCounters().stopPrinting();
                }
            }
        }
    }
}