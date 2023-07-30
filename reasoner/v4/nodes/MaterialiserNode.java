package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.logic.Materialiser;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;

import java.util.Optional;

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

    public void materialise(ActorNode<ConclusionNode> sender, ActorNode.Port port, Message.Answer msg, Rule.Conclusion conclusion) {
        assert sender == port.owner();
        Rule.Conclusion.Materialisable materialisable = conclusion.materialisable(msg.answer(), nodeRegistry.conceptManager());

        Optional<Message.Conclusion> response = Materialiser
                .materialise(materialisable, nodeRegistry.traversalEngine(), nodeRegistry.conceptManager())
                .map(materialisation -> materialisation.bindToConclusion(conclusion, msg.answer()))
                .map(conclusionAnswer -> new Message.Conclusion(msg.index(), conclusionAnswer));

        if (response.isPresent()) nodeRegistry.perfCounterFields().materialisations.add(1);

        sender.driver().execute(node -> node.receiveMaterialisation(port, response));
    }
}