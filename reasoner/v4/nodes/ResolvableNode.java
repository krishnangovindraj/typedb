package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.reasoner.common.Traversal;
import com.vaticle.typedb.core.reasoner.planner.ConjunctionGraph;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public abstract class ResolvableNode<RESOLVABLE extends Resolvable<?>, NODE extends ResolvableNode<RESOLVABLE, NODE>>
        extends ActorNode<NODE> {

    private static final Logger LOG = LoggerFactory.getLogger(ResolvableNode.class);

    protected final RESOLVABLE resolvable;
    protected final ConceptMap bounds;

    public ResolvableNode(RESOLVABLE resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<NODE> driver) {
        super(nodeRegistry, driver, () -> String.format("ResolvableNode[%s, %s]", resolvable, bounds));
        this.resolvable = resolvable;
        this.bounds = bounds;
    }

    @Override
    public String toString() {
        return String.format("%s[%s::%s]", this.getClass().getSimpleName(), this.resolvable.pattern(), this.bounds);
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
        private final ConjunctionGraph.ConjunctionNode infoNode;
        private final AnswerTable answerTable;
        private final Set<ConceptMap> seenAnswers;
        private Map<Port, Pair<Unifier, Unifier.Requirements.Instance>> conclusioNodePorts; // TODO: Improve
        private ActorNode.Port lookupPort;

        public ConcludableNode(Concludable concludable, ConceptMap bounds,
                               ConjunctionGraph.ConjunctionNode infoNode,
                               NodeRegistry nodeRegistry, Driver<ConcludableNode> driver) {
            super(concludable, bounds, nodeRegistry, driver);
            this.infoNode = infoNode;
            this.answerTable = new AnswerTable();
            this.seenAnswers = new HashSet<>();
            this.conclusioNodePorts = null;
        }

        @Override
        protected void initialise() {
            super.initialise();
            assert conclusioNodePorts == null;
            this.conclusioNodePorts = new HashMap<>();
            Driver<ConcludableLookupNode> lookupNode = nodeRegistry.createLocalNode(driver -> new ConcludableLookupNode(resolvable, bounds, nodeRegistry, driver));
            this.lookupPort = createPort(lookupNode.actor());

            nodeRegistry.logicManager().applicableRules(resolvable).forEach((rule, unifiers) -> {
                unifiers.forEach(unifier -> unifier.unify(bounds).ifPresent(boundsAndRequirements -> {
                    boolean isCyclic = infoNode.cyclicConcludables().contains(resolvable);
                    ActorNode<?> conclusionNode = nodeRegistry.conclusionSubRegistry(rule.conclusion()).getNode(boundsAndRequirements.first());
                    conclusioNodePorts.put(createPort(conclusionNode, isCyclic), new Pair<>(unifier, boundsAndRequirements.second()));
                }));
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
            conclusioNodePorts.keySet().forEach(port -> {
                if (port.state() == ActorNode.State.READY) {
                    port.readNext();
                }
            });
        }

        @Override
        public void receive(ActorNode.Port onPort, Message received) {
            switch (received.type()) {
                case ANSWER: {
                    assert onPort == lookupPort;
                    handleAnswers(Iterators.single(received.asAnswer().answer()));
                    onPort.readNext();
                    break;
                }
                case CONCLUSION: {
                    Pair<Unifier, Unifier.Requirements.Instance> unifierAndRequirements = conclusioNodePorts.get(onPort);
                    Map<Identifier.Variable, Concept> mappedBack = new HashMap<>();

                    handleAnswers(unifierAndRequirements.first()
                            .unUnify(received.asConclusion().conclusionAnswer(), unifierAndRequirements.second()));
                    onPort.readNext();
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

        private void handleAnswers(FunctionalIterator<ConceptMap> answers) {
            answers.forEachRemaining(conceptMap -> {
                if (seenAnswers.contains(conceptMap)) return;
                seenAnswers.add(conceptMap);
                // We can do this multiple times, since subscribers will be empty
                FunctionalIterator<ActorNode.Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
                Message toSend = answerTable.recordAnswer(conceptMap);
                subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
            });
        }
    }

    public static class NegatedNode extends ResolvableNode<Negated, NegatedNode> {

        private final AnswerTable answerTable;

        public NegatedNode(Negated resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<NegatedNode> driver) {
            super(resolvable, bounds, nodeRegistry, driver);
            this.answerTable = new AnswerTable();
        }

        @Override
        public void initialise() {
            super.initialise();
            resolvable.disjunction().conjunctions().forEach(conjunction -> {
                createPort(nodeRegistry.getRegistry(nodeRegistry.conjunctionStreamPlan(conjunction, bounds)).getNode(bounds));
            });
        }

        @Override
        public void readAnswerAt(Port reader, int index) {
            assert index <= 1; // Can only be
            answerTable.answerAt(index).ifPresentOrElse(
                    answer -> send(reader.owner(), reader, answer),
                    () -> propagatePull(reader, index));
        }

        private void propagatePull(Port reader, int index) {
            answerTable.registerSubscriber(reader, index);
            ports.forEach(port -> {
                assert port.state() == State.READY;
                port.readNext();
            });
        }

        @Override
        public void receive(Port port, Message message) {
            switch (message.type()) {
                case ANSWER: {
                    if (!answerTable.isComplete()) {
                        FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
                        Message toSend = answerTable.recordDone();
                        subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
                        // And we're done. No more pulling.
                    }
                    break;
                }
                case DONE: {
                    if (allPortsDone() && !answerTable.isComplete()) {
                        FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
                        Message toSend = answerTable.recordAnswer(bounds);
                        subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
                        answerTable.recordDone();
                    }
                    break;
                }
                default:
                    throw TypeDBException.of(UNIMPLEMENTED);
            }

        }
    }
}
