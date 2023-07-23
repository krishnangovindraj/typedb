package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ConjunctionNode extends ActorNode<ConjunctionNode> {

    private final List<ConceptMap> answers;
    private final ResolvableConjunction conjunction;
    private final ConceptMap bounds;

    public ConjunctionNode(ResolvableConjunction conjunction, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<ConjunctionNode> driver) {
        super(nodeRegistry, driver, debugName);
        this.conjunction = conjunction;
        this.bounds = bounds;
        answers = new ArrayList<>();
    }
}
