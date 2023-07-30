package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.reasoner.planner.ConjunctionGraph;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class ConcludableNode extends ResolvableNode<Concludable, ConcludableNode> {
    // TODO: See if I can get away without storing answers
    private final ConjunctionGraph.ConjunctionNode infoNode;
    private final AnswerTable answerTable;
    private final Set<ConceptMap> seenAnswers;
    private Map<Port, Pair<Unifier, Unifier.Requirements.Instance>> conclusioNodePorts; // TODO: Improve
    private Port lookupPort;

    public ConcludableNode(Concludable concludable, ConceptMap bounds,
                           ConjunctionGraph.ConjunctionNode infoNode,
                           NodeRegistry nodeRegistry, Driver<com.vaticle.typedb.core.reasoner.v4.nodes.ConcludableNode> driver) {
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
    public void readAnswerAt(Port reader, int index) {
        // TODO: Here, we pull on everything, and we have no notion of cyclic termination
        answerTable.answerAt(index).ifPresentOrElse(
                answer -> send(reader.owner(), reader, answer),
                () -> propagatePull(reader, index)
        );
    }

    private void propagatePull(Port reader, int index) {
        answerTable.registerSubscriber(reader, index);

        // KGFLAG: Strategy
        if (lookupPort.state() == State.READY) lookupPort.readNext();
        conclusioNodePorts.keySet().forEach(port -> {
            if (port.state() == State.READY) {
                port.readNext();
            }
        });
    }

    @Override
    public void receive(Port onPort, Message received) {
        switch (received.type()) {
            case ANSWER: {
                handleAnswer(onPort, received.asAnswer());
                break;
            }
            case CONCLUSION: {
                handleConclusion(onPort, received.asConclusion());
                break;
            }
            case CONDITIONALLY_DONE: {
                handleConditionallyDone(onPort);
                break;
            }
            case DONE: {
                handleDone(onPort);
                break;
            }
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }


    protected void handleAnswer(Port onPort, Message.Answer received) {
        assert onPort == lookupPort;
        recordAndForwardAnswers(Iterators.single(received.answer()));
        onPort.readNext();
    }

    protected void handleConclusion(Port onPort, Message.Conclusion received) {
        Pair<Unifier, Unifier.Requirements.Instance> unifierAndRequirements = conclusioNodePorts.get(onPort);
        recordAndForwardAnswers(unifierAndRequirements.first()
                .unUnify(received.conclusionAnswer(), unifierAndRequirements.second()));
        onPort.readNext();
    }

    private void handleConditionallyDone(Port onPort) {
        if (!onPort.isCyclic()) return;
        if (allPortsDoneConditionally()) {
            assert !answerTable.isConditionallyDone();
            handleAllPortsDoneConditionally();
        }
        if (onPort.state() == State.READY) onPort.readNext();
    }

    protected void handleDone(Port onPort) {
        if (allPortsDone()) {
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Message toSend = answerTable.recordDone();
            subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
        } else if (allPortsDoneConditionally()) {
            handleAllPortsDoneConditionally();
        }
    }

    private void handleAllPortsDoneConditionally() {
        if (!answerTable.isConditionallyDone()) {
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Message toSend = answerTable.recordAcyclicDone();
            subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
        }
    }

    private void recordAndForwardAnswers(FunctionalIterator<ConceptMap> answers) {
        answers.forEachRemaining(conceptMap -> {
            if (seenAnswers.contains(conceptMap)) return;
            seenAnswers.add(conceptMap);
            // We can do this multiple times, since subscribers will be empty
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Message toSend = answerTable.recordAnswer(conceptMap);
            subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
        });
    }

    public static class ConcludableLookupNode extends RetrievalNode<Concludable, ConcludableLookupNode> {
        public ConcludableLookupNode(Concludable concludable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<ConcludableLookupNode> driver) {
            super(concludable, bounds, nodeRegistry, driver);
        }
    }
}
