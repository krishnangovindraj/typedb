package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.v4.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends AbstractAcyclicNode<NODE> {

    static final Logger LOG = LoggerFactory.getLogger(ActorNode.class);

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(nodeRegistry, driver, debugName);
    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    public void readAnswerAt(ActorNode.Port reader, int index, @Nullable Integer pullerId) {
        int effectivePullerId = (pullerId != null) ? pullerId : reader.owner.nodeId;

        Optional<Message> peekAnswer = answerTable.answerAt(index);
        if (peekAnswer.isPresent()) {
            send(reader.owner, reader, peekAnswer.get());
            return;
        } else if (effectivePullerId >= nodeId) { //  strictly < would let you loop.
            send(reader.owner, reader, new Message.Snapshot(nodeId, answerTable.size()));
        } else {
            propagatePull(reader, index); // This is now effectively a 'pull'
        }
    }

    public final void readAnswerAt(ActorNode.Port reader, int index) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected abstract void handleAnswer(Port onPort, Message.Answer answer);

    protected void handleConclusion(Port onPort, Message.Conclusion conclusion) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected void handleSnapshot(Port onPort) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected abstract void handleDone(Port onPort);

    protected Port createPort(ActorNode<?> remote) {
        Port port = new Port(this, remote);
        ports.add(port);
        activePorts.add(port);
        return port;
    }

    public static class Port {

        public enum State {READY, PULLING, DONE}
        private final ActorNode<?> owner;
        private final ActorNode<?> remote;
        private State state;
        private int lastRequestedIndex;
        private boolean isPending;

        protected Port(ActorNode<?> owner, ActorNode<?> remote) {
            this.owner = owner;
            this.remote = remote;
            this.state = State.READY;
            this.lastRequestedIndex = -1;
            this.isPending = false;
        }

        protected void recordReceive(Message msg) {
            assert state == State.PULLING;
            if (msg.type() == Message.MessageType.SNAPSHOT) { // TODO
                throw TypeDBException.of(UNIMPLEMENTED);
            }

            assert lastRequestedIndex == msg.index();
            if (msg.type() == Message.MessageType.DONE) {
                state = State.DONE;
            } else if (msg.type() == Message.MessageType.SNAPSHOT) {
                this.isPending = true;
                state = State.READY;
            } else {
                state = State.READY;
            }
            assert state != State.PULLING;
        }

        public void readNext() {
            assert state == State.READY;
            state = State.PULLING;
            lastRequestedIndex += 1;
            int readIndex = lastRequestedIndex;
            remote.driver().execute(nodeActor -> nodeActor.readAnswerAt(Port.this, readIndex, null));
        }

        public ActorNode<?> owner() {
            return owner;
        }

        public ActorNode<?>  remote() {
            return remote;
        }

        public State state() {
            return state;
        }

        public int lastRequestedIndex() {
            return lastRequestedIndex;
        }

        public boolean isReady() { return state == State.READY; }
    }

}
