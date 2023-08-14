package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.v4.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends AbstractAcyclicNode<NODE> {

    static final Logger LOG = LoggerFactory.getLogger(ActorNode.class);

    private int pullingPorts;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(nodeRegistry, driver, debugName);
        pullingPorts = 0;
    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    public void readAnswerAt(ActorNode.Port reader, int index, @Nullable Integer pullerId) {
        int effectivePullerId = (pullerId != null) ? pullerId : reader.owner.nodeId;

        Optional<Message> peekAnswer = answerTable.answerAt(index);
        if (peekAnswer.isPresent()) {
            send(reader.owner, reader, peekAnswer.get());
            return;
        } else if (effectivePullerId >= nodeId) { //  strictly < would let you loop.
            NodeSnapshot nodeSnapshot = new NodeSnapshot(nodeId, answerTable.size());
            send(reader.owner, reader, new Message.Snapshot(nodeSnapshot, nodeSnapshot));
        } else {
            propagatePull(reader, index); // This is now effectively a 'pull'
        }
    }

    protected abstract void handleAnswer(Port onPort, Message.Answer answer);

    protected void handleConclusion(Port onPort, Message.Conclusion conclusion) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    private NodeSnapshot DEBUG__stateWhenLastPulled = null;

    @Override
    protected void handleDone(Port onPort) {
        if (checkTermination()) {
            onTermination();
        } else checkRetry();
    }

    protected void checkRetry() {
         // First:
         if (activePorts.isEmpty() && pullingPorts == 0) { // TODO: Find a way to pull one at a time
             assert !pendingPorts.isEmpty();
             Message.Snapshot sMinMax = findMinMaxSnapshots();
//          NodeSnapshot currentState = new NodeSnapshot(nodeId, answerTable.size());
             if ( sMinMax.oldest.nodeId >= this.nodeId ) { // older or same
                 if (sMinMax.youngest.nodeId == this.nodeId && sMinMax.youngest.answerCount == this.answerTable.size()) {
                     // TERMINATE!
                     FunctionalIterator<Port> subs = answerTable.clearAndReturnSubscribers(answerTable.size());
                     Message msg = answerTable.recordDone();
                     subs.forEachRemaining(sub -> send(sub.owner, sub, msg));
                 } else {
                     assert (DEBUG__stateWhenLastPulled == null || DEBUG__stateWhenLastPulled.answerCount < answerTable.size()); // Ah we might just have gotten more answers :/ Can't assert
                     // RESOLVE! PULL WITH OUR ID EXPLICIT!
                     this.pullerId = nodeId;
                     pendingPorts.forEach(port -> port.readNext());
                     DEBUG__stateWhenLastPulled = new NodeSnapshot(nodeId, answerTable.size());
                 }
             } else {
                 //  FORWARD SO AN ELDER CAN HANDLE
                 FunctionalIterator<Port> subs = answerTable.clearAndReturnSubscribers(answerTable.size());
                 subs.forEachRemaining(subPort -> send(subPort.owner(), subPort, sMinMax));
             }
         } // else do nothing and wait
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
    protected void handleSnapshot(Port onPort) {
        checkRetry();
    }

    private Message.Snapshot findMinMaxSnapshots() {
        NodeSnapshot sMin = null, sMax = null;
        for (Port port: pendingPorts) {
            assert port.receivedSnapshot != null;
            if (sMin == null ||  port.receivedSnapshot.youngest.compareTo(sMin) > 0) sMin = port.receivedSnapshot.youngest;
            if (sMax == null || port.receivedSnapshot.oldest.compareTo(sMax) < 0) sMax = port.receivedSnapshot.oldest;
        }
        return new Message.Snapshot(sMin, sMax);
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
        private Message.Snapshot receivedSnapshot;

        protected Port(ActorNode<?> owner, ActorNode<?> remote) {
            this.owner = owner;
            this.remote = remote;
            this.state = State.READY;
            this.lastRequestedIndex = -1;
            this.receivedSnapshot = null;
        }

        protected void recordReceive(Message msg) {
            assert state == State.PULLING;
            assert msg.type() == Message.MessageType.SNAPSHOT || lastRequestedIndex == msg.index();
            if (msg.type() == Message.MessageType.DONE) {
                state = State.DONE;
            } else if (msg.type() == Message.MessageType.SNAPSHOT) {
                this.receivedSnapshot = msg.asSnapshot();
                lastRequestedIndex -= 1; // Is this the right way to do it?
                state = State.READY;
            } else {
                state = State.READY;
            }
            assert state != State.PULLING;
            owner.pullingPorts -= 1;
        }

        public void readNext() {
            assert state == State.READY;
            state = State.PULLING;
            lastRequestedIndex += 1;
            int readIndex = lastRequestedIndex;
            owner.pullingPorts += 1;
            remote.driver().execute(nodeActor -> nodeActor.readAnswerAt(Port.this, readIndex, owner.pullerId));
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

    public static class NodeSnapshot implements Comparable<NodeSnapshot> {
        public final int nodeId;
        public final int answerCount;

        public NodeSnapshot(int nodeId, int answerCount) {
            this.nodeId = nodeId;
            this.answerCount = answerCount;
        }

        @Override
        public int compareTo(NodeSnapshot other) {
            return (this.nodeId == other.nodeId) ?
                        Integer.compare(this.answerCount, other.answerCount) :
                        Integer.compare(this.nodeId, other.nodeId);
        }
    }

}
