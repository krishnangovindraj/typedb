package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;

import java.util.Optional;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public class ConclusionNode extends ActorNode<ConclusionNode> {
    private final Rule.Conclusion conclusion;
    private final AnswerTable answerTable;
    private final ConceptMap bounds;
    private int pendingMaterialisations;

    public ConclusionNode(Rule.Conclusion conclusion, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<ConclusionNode> driver) {
        super(nodeRegistry, driver, () -> String.format("Conclusion[%s, %s, %s]", conclusion.rule(), conclusion, bounds));
        this.conclusion = conclusion;
        this.bounds = bounds;
        this.answerTable = new AnswerTable();
        this.pendingMaterialisations = 0;
    }

    @Override
    public void initialise() {
        conclusion.rule().condition().disjunction().conjunctions().forEach(conjunction -> {
            boolean isCyclic = !nodeRegistry.planner().conjunctionGraph().conjunctionNode(conjunction).cyclicConcludables().isEmpty();
            ConjunctionController.ConjunctionStreamPlan csPlan = nodeRegistry.conjunctionStreamPlan(conjunction, bounds);
            createPort(nodeRegistry.getRegistry(csPlan).getNode(bounds), isCyclic);
        });
    }

    @Override
    public void readAnswerAt(Port reader, int index) {
        answerTable.answerAt(index).ifPresentOrElse(
                answer -> send(reader.owner(), reader, answer),
                () -> propagatePull(reader, index)
        );
    }

    private void propagatePull(ActorNode.Port reader, int index) {
        answerTable.registerSubscriber(reader, index);

        // KGFLAG: Strategy
        ports.forEach(port -> {
            if (port.state() == ActorNode.State.READY) {
                port.readNext();
            }
        });
    }

    @Override
    protected void handleConditionallyDone(Port onPort) {
        if (allPortsDoneConditionally()) {
            handleAllPortsDoneConditionally();
        }
        if (onPort.state() == State.READY) onPort.readNext();
    }

    @Override
    protected void handleAnswer(Port onPort, Message.Answer answer) {
        requestMaterialisation(onPort, answer);
        // Do NOT readNext.
    }

    private void handleAllPortsDoneConditionally() {
        assert answerTable.isConditionallyDone(); // It is possible that 'cyclic' ports are conditionally done before the acyclic ones are done. but that path should handle it.
        maySendTerminationProposal();
//        throw TypeDBException.of(UNIMPLEMENTED);
    }

    private void maySendTerminationProposal() {
        assert earliestReachableNodeBirth <= birthTime && allPortsDoneConditionally();
        if (earliestReachableNodeBirth.equals(birthTime)) {
            FunctionalIterator<ActorNode.Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Message toSend = answerTable.recordTerminationProposal(new Message.TerminationProposal(birthTime, answerTable.size()));
            subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
        }
    }

    @Override
    protected void handleDone(Port onPort) {
        checkDoneAndMayForward();
    }

    private void requestMaterialisation(Port onPort, Message.Answer whenConcepts) {
        pendingMaterialisations += 1;
        nodeRegistry.materialiserNode()
                .execute(materialiserNode -> materialiserNode.materialise(this, onPort, whenConcepts, conclusion));
    }

    public void receiveMaterialisation(Port port, Optional<Message.Conclusion> thenConcepts) {
        pendingMaterialisations -= 1;
        if (thenConcepts.isPresent()) {
            FunctionalIterator<ActorNode.Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Message toSend = answerTable.recordConclusion(thenConcepts.get().conclusionAnswer());
            subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
        }
        if (port.state() == State.READY) port.readNext(); // KGFLAG: Strategy
        checkDoneAndMayForward();
    }

    private boolean checkDoneAndMayForward() {
        if (pendingMaterialisations > 0) {
            return false;
        } else if (allPortsDone() ) {
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Message toSend = answerTable.recordDone();
            subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
            return true;
        } else if (acyclicPortsDone() && !answerTable.isConditionallyDone()) { // Record acyclic done only once
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Message toSend = answerTable.recordAcyclicDone();
            subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
            // May have caused this to be true
            if (allPortsDoneConditionally()) {
                handleAllPortsDoneConditionally();
            }
            return false;
        }
        return false;
    }
}
