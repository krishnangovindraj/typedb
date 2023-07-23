package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

public class ResolvableNode extends ActorNode<ResolvableNode> {

    private final Set<ConceptMap> seenAnswers;
    private final Resolvable<?> resolvable;
    private final ConceptMap bounds;

    public ResolvableNode(Resolvable<?> resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<ResolvableNode> driver) {
        super(nodeRegistry, driver, debugName);
        this.resolvable = resolvable;
        this.bounds = bounds;
        this.seenAnswers = new HashSet<>();
    }
}
