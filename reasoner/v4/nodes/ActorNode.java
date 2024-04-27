package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.v4.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends AbstractAcyclicNode<NODE> {

    static final Logger LOG = LoggerFactory.getLogger(ActorNode.class);

    private int pullingPorts;
    private Message.HitInversion forwardedInversion;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(nodeRegistry, driver, debugName);
        pullingPorts = 0;
        forwardedInversion = null;
    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    @Override
    public void readAnswerAt(ActorNode.Port reader, int index) {
        Optional<Message> peekAnswer = answerTable.answerAt(index);
        if (peekAnswer.isPresent()) {
            send(reader.owner, reader, peekAnswer.get());
        } else if (reader.owner.nodeId >= this.nodeId) {
            send(reader.owner, reader, new Message.HitInversion(this.nodeId, true));
        } else {
            // TODO: Is this a problem? If it s already pulling, we have no clean way of handling it
            propagatePull(reader, index); // This is now effectively a 'pull'
        }
    }

    protected abstract void handleAnswer(Port onPort, Message.Answer answer);

    protected void handleConclusion(Port onPort, Message.Conclusion conclusion) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected void handleHitInversion(Port onPort, Message.HitInversion hitInversion) {
        checkInversionStatusChange();
    }

    @Override
    protected void handleDone(Port onPort) {
        if (checkTermination()) {
            onTermination();
        } else checkInversionStatusChange();
    }

    protected void checkInversionStatusChange() {
        Message.HitInversion oldestInversion = findOldestInversionStatus();
        if (!forwardedInversion.equals(oldestInversion)) {
            forwardedInversion = oldestInversion;
            // Send to all subscribers
        }
    }

    protected boolean checkTermination() {
        return allPortsDone();
    }

    protected void onTermination() {
        assert allPortsDone();
        FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
        Message toSend = answerTable.recordDone();
        subscribers.forEachRemaining(subscriber -> send(subscriber.owner(), subscriber, toSend));
    }

    private Message.HitInversion findOldestInversionStatus() {
        int oldestNodeId = Integer.MAX_VALUE;
        int oldestCount = 0;
        boolean throughAllPaths = true;
        for (Port port: pendingPorts) {
            assert port.receivedInversion != null;
            if (port.receivedInversion.nodeId < oldestNodeId) {
                oldestNodeId = port.receivedInversion.nodeId;
                oldestCount = 0;
            }
            oldestCount += 1;
            throughAllPaths = throughAllPaths && port.receivedInversion.throughAllPaths;
        }
        return new Message.HitInversion(oldestNodeId, pendingPorts.size() == oldestCount && throughAllPaths);
    }


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
        private Message.HitInversion receivedInversion;

        protected Port(ActorNode<?> owner, ActorNode<?> remote) {
            this.owner = owner;
            this.remote = remote;
            this.state = State.READY;
            this.lastRequestedIndex = -1;
            this.receivedInversion = null;
        }

        protected void recordReceive(Message msg) {
            assert state == State.PULLING;
            assert msg.type() == Message.MessageType.SNAPSHOT || lastRequestedIndex == msg.index();
            if (msg.type() == Message.MessageType.DONE) {
                state = State.DONE;
            } else if (msg.type() == Message.MessageType.SNAPSHOT) {
                this.receivedInversion = msg.asSnapshot();
                lastRequestedIndex -= 1; // Is this the right way to do it?
                state = State.READY;
            } else {
                state = State.READY;
            }
            assert state != State.PULLING;
            owner.pullingPorts -= 1;
        }

        public void readNext() {
            readNext(null);
        }

         void readNext() {
            assert state == State.READY;
            state = State.PULLING;
            lastRequestedIndex += 1;
            int readIndex = lastRequestedIndex;
            owner.pullingPorts += 1;
            remote.driver().execute(nodeActor -> nodeActor.readAnswerAt(Port.this, readIndex));
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
