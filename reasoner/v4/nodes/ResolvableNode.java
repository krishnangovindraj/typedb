package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.common.Traversal;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class ResolvableNode<RESOLVABLE extends Resolvable<?>, NODE extends ResolvableNode<RESOLVABLE, NODE>>
        extends ActorNode<NODE> {

    protected final RESOLVABLE resolvable;
    protected final ConceptMap bounds;

    public ResolvableNode(RESOLVABLE resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<NODE> driver) {
        super(nodeRegistry, driver, () -> "ResolvableNode[" + resolvable + ", " + bounds + "]");
        this.resolvable = resolvable;
        this.bounds = bounds;
    }

    public static class RetrievalNode<RES extends Resolvable<Conjunction>, RESNODE extends ResolvableNode<RES, RESNODE>>
            extends ResolvableNode<RES, RESNODE> {

        private final FunctionalIterator<ConceptMap> traversal;
        private final AnswerTable answerTable;


        public RetrievalNode(RES resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<RESNODE> driver) {
            super(resolvable, bounds, nodeRegistry, driver);
            this.traversal = Traversal.traversalIterator(nodeRegistry, resolvable.pattern(), bounds);
            this.answerTable = new AnswerTable();
        }
        @Override
        public void readAnswerAt(ActorNode.Port reader, int index) {
            answerTable.answerAt(index).ifPresentOrElse(
                    answer -> send(reader.owner(), reader, answer),
                    () -> send(reader.owner(), reader, pullTraversalSynchronous())
            );
        }

        private Message pullTraversalSynchronous() {
            return traversal.hasNext() ?
                    answerTable.recordAnswer(traversal.next()) :
                    answerTable.recordDone();
        }

        @Override
        public void receive(ActorNode.Port onPort, Message message) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    public static class RetrievableNode extends RetrievalNode<Retrievable, RetrievableNode> {
        public RetrievableNode(Retrievable retrievable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<RetrievableNode> driver) {
            super(retrievable, bounds, nodeRegistry, driver);
        }
    }


    public static class ConcludableLookupNode extends RetrievalNode<Concludable, ConcludableLookupNode> {
        public ConcludableLookupNode(Concludable concludable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<ConcludableLookupNode> driver) {
            super(concludable, bounds, nodeRegistry, driver);
        }
    }

    public static class ConcludableNode extends ResolvableNode<Concludable, ConcludableNode> {
        // TODO: See if I can get away without storing answers
        private final AnswerTable answerTable;
        private final Set<ConceptMap> seenAnswers;
        private Map<Port, Pair<ConceptMap, Unifier.Requirements.Instance>> conditionNodePorts; // TODO: Improve
        private ActorNode.Port lookupPort;

        public ConcludableNode(Concludable concludable, ConceptMap bounds,
                               NodeRegistry nodeRegistry, Driver<ConcludableNode> driver) {
            super(concludable, bounds, nodeRegistry, driver);
            this.answerTable = new AnswerTable();
            this.seenAnswers = new HashSet<>();
            this.conditionNodePorts = null;
        }

        @Override
        protected void initialise() {
            super.initialise();
            assert conditionNodePorts == null;
            this.conditionNodePorts = new HashMap<>();
            Driver<ConcludableLookupNode> lookupNode = nodeRegistry.createLocalNode(driver -> new ConcludableLookupNode(resolvable, bounds, nodeRegistry, driver));
            this.lookupPort = createPort(lookupNode.actor());

            nodeRegistry.logicManager().applicableRules(resolvable).forEach((rule, unifiers) -> {
                rule.condition().disjunction().conjunctions().forEach(conjunction -> {
                    unifiers.forEach(unifier -> unifier.unify(bounds).ifPresent(boundsAndRequirements -> {
                        ConjunctionController.ConjunctionStreamPlan csPlan = nodeRegistry.conjunctionStreamPlan(conjunction, boundsAndRequirements.first());
                        ActorNode<?> conditionNode = nodeRegistry.getRegistry(csPlan).getNode(boundsAndRequirements.first());
                        conditionNodePorts.put(createPort(conditionNode), boundsAndRequirements);
                    }));
                });
            });
        }

        @Override
        public void readAnswerAt(ActorNode.Port reader, int index) {
            // TODO: Here, we pull on everything, and we have no notion of cyclic termination
            answerTable.answerAt(index).ifPresentOrElse(
                    answer -> send(reader.owner(), reader, answer),
                    () -> propagatePull(reader, index)
            );
        }

        private void propagatePull(ActorNode.Port reader, int index) {
            answerTable.registerSubscriber(reader, index);

            // KGFLAG: Strategy
            if (lookupPort.state() == ActorNode.State.READY) lookupPort.readNext();
            conditionNodePorts.keySet().forEach(port -> {
                if (port.state() == ActorNode.State.READY) {
                    port.readNext();
                }
            });
        }

        @Override
        public void receive(ActorNode.Port onPort, Message received) {
            switch (received.type()) {
                case ANSWER: {
                    if (seenAnswers.contains(received.answer().get())) return;
                    seenAnswers.add(received.answer().get());
                    FunctionalIterator<ActorNode.Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
                    Message toSend = answerTable.recordAnswer(received.answer().get());
                    subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));

                    onPort.readNext(); // KGFLAG: Strategy
                    break;
                }
                case DONE: {
                    if (allPortsDone()) {
                        FunctionalIterator<ActorNode.Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
                        Message toSend = answerTable.recordDone();
                        subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
                    }
                    break;
                }
                default:
                    throw TypeDBException.of(ILLEGAL_STATE);
            }
        }
    }
}
