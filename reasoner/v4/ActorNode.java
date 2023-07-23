package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;

import java.util.function.Supplier;

public abstract class ActorNode<NODE extends ActorNode<NODE>>  extends Actor<NODE> {

    protected final NodeRegistry nodeRegistry;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(driver, debugName);
        this.nodeRegistry = nodeRegistry;
    }

    public abstract void readAnswerAt(ActorNode<?> sender, int index);

    public abstract void receive(ActorNode<?> sender, Message message);

}
