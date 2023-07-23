package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public class ConjunctionNode extends ActorNode<ConjunctionNode> {

    private final List<ConceptMap> answers;
    private final ResolvableConjunction conjunction;
    private final ConceptMap bounds;

    public ConjunctionNode(ResolvableConjunction conjunction, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<ConjunctionNode> driver) {
        super(nodeRegistry, driver, () -> "ConjunctionNode[" + conjunction + ", " + bounds + "]");
        this.conjunction = conjunction;
        this.bounds = bounds;
        answers = new ArrayList<>();
    }

    @Override
    public void readAnswerAt(ActorNode<?> sender, int index) {
        assert index <= answers.size();
        throw TypeDBException.of(UNIMPLEMENTED);
    }

    @Override
    public void receive(ActorNode<?> sender, Message message) {
        throw TypeDBException.of(UNIMPLEMENTED);
    }
}
