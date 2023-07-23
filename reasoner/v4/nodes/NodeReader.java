package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;

import java.util.HashMap;
import java.util.Map;

import static com.vaticle.typedb.core.reasoner.v4.nodes.NodeReader.Status.PULLING;
import static com.vaticle.typedb.core.reasoner.v4.nodes.NodeReader.Status.DONE;
import static com.vaticle.typedb.core.reasoner.v4.nodes.NodeReader.Status.READY;

public class NodeReader {

    enum Status { READY, PULLING, DONE }
    private final ActorNode<?> owner;
    Map<ActorNode<?>, State> states;
    private int doneCount;

    public NodeReader(ActorNode<?> owner) {
        this.owner = owner;
        this.states = new HashMap<>();
        this.doneCount = 0;
    }

    public boolean allDone() {
        return doneCount == states.size();
    }

    public void addSource(ActorNode<?> node) {
        states.put(node, new State());
    }

    public Status status(ActorNode<?> node) {
        return states.get(node).status;
    }

    public void readNext(ActorNode<?> node) {
        State nodeState = states.get(node);
        assert nodeState.status == READY;
        nodeState.status = PULLING;
        node.driver().execute(nodeActor -> nodeActor.readAnswerAt(this.owner, nodeState.nextIndex));
    }

    public void recordReceive(ActorNode<?> node, Message msg) {
        State state = states.get(node);
        assert state.nextIndex == msg.index();
        state.nextIndex += 1;
        if (msg.type() == Message.MessageType.DONE) {
            state.status = DONE;
            doneCount += 1;
        } else {
            state.status = READY;
        }
    }

    private static class State {
        private NodeReader.Status status;
        private int nextIndex;

        private State() {
            this.status = READY;
            this.nextIndex = 0;
        }
    }
}
