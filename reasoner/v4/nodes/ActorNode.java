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
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends AbstractAcyclicNode<NODE> {

    static final Logger LOG = LoggerFactory.getLogger(ActorNode.class);

    private int pullingPorts;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(nodeRegistry, driver, debugName);
        pullingPorts = 0;
    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    @Override
    public void readAnswerAt(ActorNode.Port reader, int index, @Nullable Integer pullerId) {

        int effectivePullerId;
        if (pullerId != null) {
            effectivePullerId = pullerId;
            if (pullerId > (this.pullerId != null ? this.pullerId : this.nodeId )) this.pullerId = pullerId;
        } else {
            effectivePullerId = reader.owner.nodeId;
        }

        Optional<Message> peekAnswer = answerTable.answerAt(index);
        if (peekAnswer.isPresent()) {
            send(reader.owner, reader, peekAnswer.get());
            return;
        } else if (NodeSnapshot.youngerOrSameNode_cannotPullThrough(effectivePullerId, nodeId)) {
            NodeSnapshot nodeSnapshot = currentSnapshot();
            send(reader.owner, reader, new Message.Snapshot(nodeSnapshot, nodeSnapshot));
        } else {
            // TODO: Is this a problem? If it s already pulling, we have no clean way of handling it
            propagatePull(reader, index); // This is now effectively a 'pull'
        }
    }

    protected abstract void handleAnswer(Port onPort, Message.Answer answer);

    protected void handleConclusion(Port onPort, Message.Conclusion conclusion) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected void handleSnapshot(Port onPort, Message.Snapshot snapshot) {
        if (NodeSnapshot.youngerOrSameButDifferentAnswers(snapshot.youngest, currentSnapshot())) {
            assert pullerId == null; // This could have happened once, because we subscribe to an existing pull even when pulling with a pullerId.
            onPort.readNext(nodeId);
        }
        checkRetry();
    }

    @Override
    protected void handleDone(Port onPort) {
        if (checkTermination()) {
            onTermination();
        } else checkRetry();
    }

    protected void checkRetry() {
         // First:
        if (!activePorts.isEmpty() || pullingPorts > 0) return; //
        NodeSnapshot currentSnapshot = currentSnapshot();
        Message.Snapshot snapshotAcrossEdges = findYoungestOldestSnapshotsOnPorts();

        assert !pendingPorts.isEmpty();
        assert !NodeSnapshot.youngerOrSameButDifferentAnswers(snapshotAcrossEdges.youngest, currentSnapshot);
        if ( NodeSnapshot.same(snapshotAcrossEdges.oldest, currentSnapshot) ) {
             FunctionalIterator<Port> subs = answerTable.clearAndReturnSubscribers(answerTable.size());
             Message msg = answerTable.recordDone();
             subs.forEachRemaining(sub -> send(sub.owner, sub, msg));
        } else {
             //  FORWARD SO AN ELDER CAN HANDLE
             FunctionalIterator<Port> subs = answerTable.clearAndReturnSubscribers(answerTable.size());
             subs.forEachRemaining(subPort -> send(subPort.owner(), subPort, snapshotAcrossEdges));
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

    private NodeSnapshot currentSnapshot() {
        return new NodeSnapshot(nodeId, answerTable.size());
    }

    private Message.Snapshot findYoungestOldestSnapshotsOnPorts() {
        NodeSnapshot youngest = null, oldest = null;
        for (Port port: pendingPorts) {
            assert port.receivedSnapshot != null;
            if (youngest == null ||  NodeSnapshot.youngerOrSameButDifferentAnswers(port.receivedSnapshot.youngest, youngest)) {
                youngest = port.receivedSnapshot.youngest;
            }
            if (oldest == null || NodeSnapshot.older(port.receivedSnapshot.oldest, oldest)) oldest = port.receivedSnapshot.oldest;
        }
        return new Message.Snapshot(youngest, oldest);
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
            readNext(null);
        }

         void readNext(@Nullable Integer pullWithIdOverride) {
            assert state == State.READY;
            state = State.PULLING;
            lastRequestedIndex += 1;
            int readIndex = lastRequestedIndex;
            owner.pullingPorts += 1;
            Integer pullWithId = pullWithIdOverride != null ? pullWithIdOverride : owner.pullerId;
            remote.driver().execute(nodeActor -> nodeActor.readAnswerAt(Port.this, readIndex, pullWithId));
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

        public static boolean youngerOrSameNode_cannotPullThrough(int firstNodeId, int secondNodeId) {
            return firstNodeId >= secondNodeId;
        }

        @Override
        public int compareTo(NodeSnapshot other) {
            return (this.nodeId == other.nodeId) ?
                        Integer.compare(this.answerCount, other.answerCount) :
                        Integer.compare(other.nodeId, this.nodeId); // LowerIds are older
        }

        public static boolean older(NodeSnapshot first, NodeSnapshot second) {
            return first.compareTo(second) > 0;
        }

        public static boolean youngerOrSameButDifferentAnswers(NodeSnapshot first, NodeSnapshot second) {
            return first.compareTo(second) < 0;
        }

        public static boolean same(NodeSnapshot first, NodeSnapshot second) {
            return first.compareTo(second) == 0;
        }
    }
}
