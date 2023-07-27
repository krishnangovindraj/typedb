package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.logic.Materialiser;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public class MaterialiserNode extends Actor<MaterialiserNode> {
    // Does not extend ActorNode
    private final NodeRegistry nodeRegistry;

    public MaterialiserNode(NodeRegistry nodeRegistry, Driver<MaterialiserNode> driver) {
        super(driver, () -> "MaterialiserNode");
        this.nodeRegistry = nodeRegistry;
    }

    @Override
    protected void exception(Throwable e) {
        nodeRegistry.terminate(e);
    }

    public void materialise(ActorNode.Port sender, Message.Answer msg, Rule.Conclusion conclusion) {
        Rule.Conclusion.Materialisable materialisable = conclusion.materialisable(msg.answer(), nodeRegistry.conceptManager());

        Optional<Message.Answer> response;
        Optional<Map<Identifier.Variable, Concept>> toBeHackedBack = Materialiser
                .materialise(materialisable, nodeRegistry.traversalEngine(), nodeRegistry.conceptManager())
                .map(materialisation -> materialisation.bindToConclusion(conclusion, msg.answer()));
                if (toBeHackedBack.isEmpty()) {
                    response = Optional.empty();
                }else {
                    Map<Identifier.Variable.Retrievable, Concept> hackBackToConceptMap = new HashMap<>();
                    toBeHackedBack.get().forEach((k,v) -> {
                        if (k.isRetrievable()) throw TypeDBException.of(UNIMPLEMENTED);
                        hackBackToConceptMap.put(k.asRetrievable(), v);
                    });
                    response = Optional.of(Message.answer(msg.index(), new ConceptMap(hackBackToConceptMap)));
                }
        sender.owner().driver().execute(node -> node.receiveMaterialisation(sender, response));
    }
}
