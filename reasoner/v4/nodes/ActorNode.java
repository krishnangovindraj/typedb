package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.v4.Request;
import com.vaticle.typedb.core.reasoner.v4.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends AbstractAcyclicNode<NODE> {

    static final Logger LOG = LoggerFactory.getLogger(ActorNode.class);

    private final List<ActorNode.Port> downstreamPorts;
    private Response.Candidacy forwardedCandidacy;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(nodeRegistry, driver, debugName);
        forwardedCandidacy = null;
        downstreamPorts = new ArrayList<>();
    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    @Override
    protected void readAnswerAt(ActorNode.Port reader, int index) {
        Optional<Response> peekAnswer = answerTable.answerAt(index);
        if (peekAnswer.isPresent()) {
            sendResponse(reader.owner, reader, peekAnswer.get());
        } else if (reader.owner.nodeId >= this.nodeId) {
            sendResponse(reader.owner, reader, new Response.Candidacy(this.nodeId, this.answerTable.size()));
        } else {
            // TODO: Is this a problem? If it s already pulling, we have no clean way of handling it
            propagatePull(reader, index); // This is now effectively a 'pull'
        }
    }

    protected abstract void handleAnswer(Port onPort, Response.Answer answer);

    protected void handleConclusion(Port onPort, Response.Conclusion conclusion) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected void handleCandidacy(Port onPort, Response.Candidacy candidacy) {
        checkCandidacyStatusChange(); // We may still have to forward a better tableSize to everyone.
        if (candidacy.nodeId == this.nodeId) {
            // Grow tree on port
            onPort.sendRequest(new Request.GrowTree(this.nodeId, this.nodeId));
        }
    }

    @Override
    protected void handleDone(Port onPort) {
        if (checkTermination()) {
            onTermination();
        } else checkCandidacyStatusChange();
    }

    protected void checkCandidacyStatusChange() {
        Optional<Response.Candidacy> oldestCandidate = findOldestCandidate(); // TODO: This can be a single field that only needs to be re-evaluated in the case of an upstream termination
        if (oldestCandidate.isEmpty()) return;
        if (forwardedCandidacy == null || !forwardedCandidacy.equals(oldestCandidate.get())) {
            forwardedCandidacy = oldestCandidate.get();
            downstreamPorts.forEach(port -> sendResponse(port.owner, port, forwardedCandidacy));
        }
    }

    protected boolean checkTermination() {
        return allPortsDone();
    }

    protected void onTermination() {
        assert allPortsDone();
        FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
        Response toSend = answerTable.recordDone();
        subscribers.forEachRemaining(subscriber -> sendResponse(subscriber.owner(), subscriber, toSend));
    }

    private Optional<Response.Candidacy> findOldestCandidate() {
        return activePorts.stream().map(port -> port.receivedCandidacy).filter(Objects::nonNull)
                .min(Comparator.comparingInt(candidacy -> candidacy.nodeId));
    }


    protected Port createPort(ActorNode<?> remote) {
        Port port = new Port(this, remote);
        remote.notifyPortCreated(port);
        ports.add(port);
        activePorts.add(port);
        return port;
    }

    private void notifyPortCreated(Port downstream) {
        // TODO: Not thread safe! Consider using a HELLO request instead
        this.downstreamPorts.add(downstream);
    }

    public static class Port {

        public enum State {READY, PULLING, DONE}
        private final ActorNode<?> owner;
        private final ActorNode<?> remote;
        private State state;
        private int lastRequestedIndex;
        private Response.Candidacy receivedCandidacy;

        protected Port(ActorNode<?> owner, ActorNode<?> remote) {
            this.owner = owner;
            this.remote = remote;
            this.state = State.READY;
            this.lastRequestedIndex = -1;
            this.receivedCandidacy = null;
        }

        protected void recordReceive(Response msg) {
            assert msg.type() == Response.ResponseType.CANDIDACY || (state == State.PULLING && lastRequestedIndex == msg.index());
            if (msg.type() == Response.ResponseType.DONE) {
                state = State.DONE;
            } else if (msg.type() == Response.ResponseType.CANDIDACY) {
                this.receivedCandidacy = msg.asCandidacy();
                // Don't stop on pulling on a candidacy
            } else {
                state = State.READY;
            }
        }


        public void readNext() {
            assert state == State.READY;
            state = State.PULLING;
            lastRequestedIndex += 1;
            int readIndex = lastRequestedIndex;
            sendRequest(new Request.ReadAnswer(readIndex));
        }

        private void sendRequest(Request request) {
            remote.driver().execute(nodeActor -> nodeActor.receiveRequest(this, request));
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
