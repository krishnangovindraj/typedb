package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Materialiser;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.reasoner.controller.ConjunctionController;
import com.vaticle.typedb.core.reasoner.v4.Response;

import java.util.Optional;

public class ConclusionNode extends ActorNode<ConclusionNode> {
    private final Rule.Conclusion conclusion;
    private final ConceptMap bounds;

    public ConclusionNode(Rule.Conclusion conclusion, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<ConclusionNode> driver) {
        super(nodeRegistry, driver, () -> String.format("Conclusion[%s, %s, %s]", conclusion.rule(), conclusion, bounds));
        this.conclusion = conclusion;
        this.bounds = bounds;
    }

    @Override
    public void initialise() {
        super.initialise();
        conclusion.rule().condition().disjunction().conjunctions().forEach(conjunction -> {
            ConjunctionController.ConjunctionStreamPlan csPlan = nodeRegistry.conjunctionStreamPlan(conjunction, bounds);
            createPort(nodeRegistry.getRegistry(csPlan).getNode(bounds));
        });
    }

    protected void propagatePull(ActorNode.Port reader, int index) {
        answerTable.registerSubscriber(reader, index);

        // KGFLAG: Strategy
        ports.forEach(port -> {
            if (port.isReady()) port.readNext();
        });
    }

    @Override
    protected void handleAnswer(Port onPort, Response.Answer answer) {
        Optional<Response.Conclusion> thenConcepts = materialise(this.nodeRegistry, answer, conclusion);
        if (thenConcepts.isPresent()) {
            FunctionalIterator<ActorNode.Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Response toSend = answerTable.recordConclusion(thenConcepts.get().conclusionAnswer());
            subscribers.forEachRemaining(subscriber -> sendResponse(subscriber.owner(), subscriber, toSend));
        }
        if (onPort.isReady()) onPort.readNext();
    }

    private static synchronized Optional<Response.Conclusion> materialise(NodeRegistry nodeRegistry, Response.Answer msg, Rule.Conclusion conclusion) {
        Rule.Conclusion.Materialisable materialisable = conclusion.materialisable(msg.answer(), nodeRegistry.conceptManager());
        Optional<Response.Conclusion> response = Materialiser
                .materialise(materialisable, nodeRegistry.traversalEngine(), nodeRegistry.conceptManager())
                .map(materialisation -> materialisation.bindToConclusion(conclusion, msg.answer()))
                .map(conclusionAnswer -> new Response.Conclusion(msg.index(), conclusionAnswer));

        if (response.isPresent()) nodeRegistry.perfCounterFields().materialisations.add(1);
        return response;
    }
}
