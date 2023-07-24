package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.common.collection.Collections;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController.ConjunctionStreamPlan.CompoundStreamPlan;
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

public class ConjunctionNode extends ActorNode<ConjunctionNode> {

    private static final Logger LOG = LoggerFactory.getLogger(ConjunctionNode.class);
    private final ResolvableConjunction conjunction;
    private final ConceptMap bounds;
    private final CompoundStreamPlan compoundStreamPlan;
    private final AnswerTable answerTable;
    private final NodeReader readingDelegate;
    private ActorNode<?> leftChild;
    private Map<ActorNode<?>, Set<ConceptMap>> rightChildExtensions;


    public ConjunctionNode(ResolvableConjunction conjunction, ConceptMap bounds, CompoundStreamPlan compoundStreamPlan, NodeRegistry nodeRegistry, Driver<ConjunctionNode> driver) {
        super(nodeRegistry, driver, () -> "ConjunctionNode[" + conjunction + ", " + bounds + "]");
        this.conjunction = conjunction;
        this.bounds = bounds;
        this.compoundStreamPlan = compoundStreamPlan;
        this.answerTable = new AnswerTable();
        this.readingDelegate = new NodeReader(this);
    }

    private void ensureInitialised() {
        if (this.leftChild == null) {
            this.leftChild = nodeRegistry.getRegistry(leftPlan()).getNode(bounds);
            readingDelegate.addSource(this.leftChild);
            this.rightChildExtensions = new HashMap<>();
        }
    }

    @Override
    public void readAnswerAt(ActorNode<?> reader, int index) {
        ensureInitialised();
        // TODO: Here, we pull on everything, and we have no notion of cyclic termination
        answerTable.answerAt(index).ifPresentOrElse(
                answer -> send(reader, answer),
                () -> propagatePull(reader, index)
        );
    }

    private void propagatePull(ActorNode<?> reader, int index) {
        answerTable.registerSubscriber(reader, index);

        // KGFLAG: Strategy
        if (readingDelegate.status(leftChild) == NodeReader.Status.READY) {
            readingDelegate.readNext(leftChild);
        }
        rightChildExtensions.keySet().forEach(source -> {
            if (readingDelegate.status(source) == NodeReader.Status.READY) {
                readingDelegate.readNext(source);
            }
        });
    }

    @Override
    public void receive(ActorNode<?> sender, Message received) {
        LOG.info("Received {}@{} from {}",
                received.answer().map(a -> a.toString()).orElse(received.type().toString()),
                received.index(), sender.debugName().get() );
        readingDelegate.recordReceive(sender, received);
        switch (received.type()) {
            case ANSWER: {
                if (sender == leftChild) receiveLeft(sender, received.answer().get());
                else receiveRight(sender, received.answer().get());
            }
            case DONE: {
                if (readingDelegate.allDone()) {
                    FunctionalIterator<ActorNode<?>> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
                    Message toSend = answerTable.recordDone();
                    subscribers.forEachRemaining(subscriber -> send(subscriber, toSend));
                }
                break;
            }
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private void receiveLeft(ActorNode<?> sender, ConceptMap answer) {
        assert sender == leftChild;
        Set<Identifier.Variable.Retrievable> extensionVars = Collections.intersection(answer.concepts().keySet(), compoundStreamPlan.outputs());
        ActorNode<?> newRightChild = nodeRegistry.getRegistry(rightPlan()).getNode(answer.filter(rightPlan().identifiers()));
        if (!rightChildExtensions.containsKey(newRightChild)) {
            readingDelegate.addSource(newRightChild);
            readingDelegate.readNext(sender); // KGFLAG: Strategy
        } else {
            FunctionalIterator<ActorNode<?>> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            TODO: Figure out how to merge left answers with existing right answers
                    Maybe create multiple readers for a child?
        }
        rightChildExtensions.get(newRightChild).add(answer.filter(extensionVars));
        if (readingDelegate.status(newRightChild) != NodeReader.Status.PULLING) readingDelegate.readNext(newRightChild);
    }

    public void receiveRight(ActorNode<?> sender, ConceptMap answer) {
        FunctionalIterator<ActorNode<?>> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());

        int firstAnswerIndex = answerTable.size();
        rightChildExtensions.get(sender).forEach(extension -> {
            ConceptMap extendedAnswer = merge(extension, answer);
            answerTable.recordAnswer(extendedAnswer);
        });
        Message toSend = answerTable.answerAt(firstAnswerIndex).get();
        subscribers.forEachRemaining(subscriber -> send(subscriber, toSend));
        readingDelegate.readNext(sender); // KGFLAG: Strategy
    }

    private ConceptMap merge(ConceptMap into, ConceptMap from) {
        Map<Identifier.Variable.Retrievable, Concept> compounded = new HashMap<>(into.concepts());
        compounded.putAll(from.concepts());
        return new ConceptMap(compounded, into.explainables().merge(from.explainables()));
    }

    private ConjunctionController.ConjunctionStreamPlan leftPlan() { return compoundStreamPlan.childAt(0); }
    private ConjunctionController.ConjunctionStreamPlan rightPlan() { return compoundStreamPlan.childAt(1); }

//    // Test retrievable
//    private ActorNode<?> subscriber;
//    @Override
//    public void readAnswerAt(ActorNode<?> reader, int index) {
//        subscriber = reader;
//        Retrievable retrievable = nodeRegistry.logicManager().compile(conjunction).stream().findFirst().get().asRetrievable();
//        nodeRegistry.retrievableSubRegistry(retrievable).getNode(new ConceptMap()).driver().execute(actor -> actor.readAnswerAt(this, index));
//    }
//    @Override
//    public void receive(ActorNode<?> sender, Message message) {
//        send(subscriber, message);
//    }
}
