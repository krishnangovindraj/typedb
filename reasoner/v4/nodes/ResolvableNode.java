package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;

import java.util.HashSet;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public class ResolvableNode extends ActorNode<ResolvableNode> {

    private final Set<ConceptMap> seenAnswers;
    private final Resolvable<?> resolvable;
    private final ConceptMap bounds;

    public ResolvableNode(Resolvable<?> resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<ResolvableNode> driver) {
        super(nodeRegistry, driver, () -> "ResolvableNode[" + resolvable + ", " + bounds + "]");
        this.resolvable = resolvable;
        this.bounds = bounds;
        this.seenAnswers = new HashSet<>();
    }

    @Override
    protected void exception(Throwable e) {
        throw TypeDBException.of(UNIMPLEMENTED);
    }

    @Override
    public void readAnswerAt(ActorNode<?> sender, int index) {
        throw TypeDBException.of(UNIMPLEMENTED);
    }

    @Override
    public void receive(ActorNode<?> sender, Message message) {
        throw TypeDBException.of(UNIMPLEMENTED);
    }
}
