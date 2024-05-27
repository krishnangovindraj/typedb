package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.reasoner.v4.Response;

public class NegatedNode extends ResolvableNode<Negated, NegatedNode> {


    public NegatedNode(Negated resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<com.vaticle.typedb.core.reasoner.v4.nodes.NegatedNode> driver) {
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
            assert port.isReady();
            port.readNext();
        });
    }

    @Override
    protected void handleAnswer(Port onPort, Response.Answer asAnswer) {
        if (!answerTable.isComplete()) {
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            nodeRegistry.notiyNodeTermination(this.nodeId);
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
        if (!answerTable.isComplete()) {
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Response toSend = answerTable.recordAnswer(bounds);
            nodeRegistry.notiyNodeTermination(this.nodeId);
            subscribers.forEachRemaining(subscriber -> sendResponse(subscriber.owner(), subscriber, toSend));
            answerTable.recordDone();
            System.out.printf("TERMINATE: Node[%d] has terminated\n", this.nodeId);
        }
    }
}
