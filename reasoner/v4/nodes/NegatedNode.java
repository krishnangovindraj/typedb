package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public class NegatedNode extends ResolvableNode<Negated, NegatedNode> {

    private final AnswerTable answerTable;

    public NegatedNode(Negated resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<com.vaticle.typedb.core.reasoner.v4.nodes.NegatedNode> driver) {
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
