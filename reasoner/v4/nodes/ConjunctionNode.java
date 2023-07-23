package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;

import java.util.ArrayList;
import java.util.List;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public class ConjunctionNode extends ActorNode<ConjunctionNode> {

    private final List<ConceptMap> answers;
    private final ResolvableConjunction conjunction;
    private final ConceptMap bounds;
    private final ConjunctionController.ConjunctionStreamPlan conjunctionStreamPlan;

    public ConjunctionNode(ResolvableConjunction conjunction, ConceptMap bounds, ConjunctionController.ConjunctionStreamPlan conjunctionStreamPlan, NodeRegistry nodeRegistry, Driver<ConjunctionNode> driver) {
        super(nodeRegistry, driver, () -> "ConjunctionNode[" + conjunction + ", " + bounds + "]");
        this.conjunction = conjunction;
        this.bounds = bounds;
        this.conjunctionStreamPlan = conjunctionStreamPlan;
        this.answers = new ArrayList<>();
    }

    @Override
    public void readAnswerAt(ActorNode<?> reader, int index) {
        assert index <= answers.size();
        /// TODO
        throw TypeDBException.of(UNIMPLEMENTED);
    }

    @Override
    public void receive(ActorNode<?> sender, Message message) {
        throw TypeDBException.of(UNIMPLEMENTED);
    }

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
