package com.vaticle.typedb.core.reasoner.nodes;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.reasoner.messages.Response;

public class NegatedNode extends ResolvableNode<Negated, NegatedNode> {


    public NegatedNode(Negated resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<com.vaticle.typedb.core.reasoner.nodes.NegatedNode> driver) {
        super(resolvable, bounds, nodeRegistry, driver);
    }

    @Override
    public void initialise() {
        super.initialise();
        resolvable.disjunction().conjunctions().forEach(conjunction -> {
            ConceptMap conjBounds = bounds.filter(conjunction.pattern().retrieves());
            createPort(nodeRegistry.getRegistry(nodeRegistry.conjunctionStreamPlan(conjunction, conjBounds)).getNode(conjBounds));
        });
    }

    @Override
    public void computeNextAnswer(Port reader, int index) {
        assert index == 0 && answerTable.answerAt(index).isEmpty();
        answerTable.registerSubscriber(reader, index);
        ports.forEach(port -> {
//            assert port.isReady();
//            port.readNext();
            if (port.isReady()) port.readNext();
        });
    }

    @Override
    protected void handleAnswer(Port onPort, Response.Answer asAnswer) {
        if (!answerTable.isComplete()) {
            terminationTracker.notifyTermination();
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Response toSend = answerTable.recordDone();
            subscribers.forEachRemaining(subscriber -> sendResponse(subscriber.owner(), subscriber, toSend));
            // And we're done. No more pulling.
        }
    }

    @Override
    protected void handleDone(Port onPort) {
        if (!answerTable.isComplete()) super.handleDone(onPort);
    }

    @Override
    protected void onTermination() {
        terminationTracker.notifyTermination();
        if (!answerTable.isComplete()) {
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Response toSend = answerTable.recordAnswer(bounds);
            subscribers.forEachRemaining(subscriber -> sendResponse(subscriber.owner(), subscriber, toSend));
            answerTable.recordDone();
            trace("TERMINATE: Node[%d] has terminated", this.nodeId);
        }
    }
}
