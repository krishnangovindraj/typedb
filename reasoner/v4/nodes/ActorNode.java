package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.v4.Request;
import com.vaticle.typedb.core.reasoner.v4.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends AbstractAcyclicNode<NODE> {

    static final Logger LOG = LoggerFactory.getLogger(ActorNode.class);

    private final List<ActorNode.Port> downstreamPorts;
    private Response.Candidacy forwardedCandidacy;
    private Request.GrowTree treeAncestor;
    private Response.TreeVote treeVote;
    private Port treeAncestorPort;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(nodeRegistry, driver, debugName);
        forwardedCandidacy = null;
        downstreamPorts = new ArrayList<>();
        treeAncestor = new Request.GrowTree(this.nodeId); // TODO: Restoring when state changes weep ;_;
        treeVote = null;
        treeAncestorPort = null;
    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    @Override
    protected void readAnswerAt(ActorNode.Port reader, Request.ReadAnswer readAnswerRequest) {
        int index = readAnswerRequest.index;
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

    protected void growTree(ActorNode.Port requestorPort, Request.GrowTree growTreeRequest) {
        if (growTreeRequest.root != forwardedCandidacy.nodeId)  return;
        if (treeAncestor == null || growTreeRequest.root != treeAncestor.root ) {
            treeAncestor = growTreeRequest; // TODO: FORWARD!
            treeAncestorPort = requestorPort;
            activePorts.stream().filter(port -> port.receivedCandidacy != null && port.receivedCandidacy.nodeId == treeAncestor.root)
                    .forEach(port -> port.sendRequest(growTreeRequest));
        } else {
            assert growTreeRequest.root == treeAncestor.root; // We already have an ancestor. WHAT DO? We need a way of telling them not to wait for us.
            // Cheeky, I'm just going to send them a MAXINT for the tableSIze. If we change our vote, the leader will hear of it through our ancestor.
            sendResponse(requestorPort.owner, requestorPort, new Response.TreeVote(new Response.Candidacy(growTreeRequest.root, Integer.MAX_VALUE)));
        }
    }

    protected void terminateSCC(ActorNode.Port requestorPort, Request.TerminateSCC terminateSCC) {
        if (requestorPort == treeAncestorPort) {
            activePorts.forEach(port -> port.sendRequest(terminateSCC));
            onTermination(); // A negated node can just terminate and let answers stream in later. We do the same.
        }
    }


    // Response handling

    protected abstract void handleAnswer(Port onPort, Response.Answer answer);

    protected void handleConclusion(Port onPort, Response.Conclusion conclusion) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    protected void handleCandidacy(Port onPort, Response.Candidacy candidacy) {
        checkCandidacyStatusChange(); // We may still have to forward a better tableSize to everyone.
        if (candidacy.nodeId == this.treeAncestor.root) {
            // Grow tree on port
            onPort.sendRequest(new Request.GrowTree(this.treeAncestor.root));
        }
    }

    protected void handleTreeVote(Port onPort, Response.TreeVote treeVote) {
        checkTreeStatusChange();
    }

    @Override
    protected void handleDone(Port onPort) {
        if (checkTermination()) {
            onTermination();
        } else {
            checkCandidacyStatusChange();
            checkTreeStatusChange();
        }
    }

    protected void checkCandidacyStatusChange() {
        Optional<Response.Candidacy> oldestCandidate = activePorts.stream().map(port -> port.receivedCandidacy).filter(Objects::nonNull) // Ok to filter here, but not for votes.
                .min(Response.Candidacy.Comparator); // TODO: Maybe these can be a single field that only needs to be re-evaluated in the case of an upstream termination
        if (oldestCandidate.isEmpty()) return;
        if (forwardedCandidacy == null || !forwardedCandidacy.equals(oldestCandidate.get())) {
            forwardedCandidacy = oldestCandidate.get();
            downstreamPorts.forEach(port -> sendResponse(port.owner, port, forwardedCandidacy));
        }
    }

    private void checkTreeStatusChange() {
        if (forwardedCandidacy != null && treeAncestor.root == forwardedCandidacy.nodeId) {
            if (treeVote != null && forwardedCandidacy == treeVote.voteFor) return; // Our vote is up-to-date.
            // We may have to vote.
            // For voting, treeChildren is necessarily equal to activePorts. A port that's not in the tree will not have voted for the candidate.
            boolean mustVote = activePorts.stream().allMatch(treeChild -> treeChild.receivedTreeVote != null && treeChild.receivedTreeVote.supports(forwardedCandidacy));
            if (mustVote) { // No outstanding ports
                treeVote = new Response.TreeVote(forwardedCandidacy);
                if (forwardedCandidacy.nodeId == this.nodeId) {
                    assert forwardedCandidacy.tableSize == this.answerTable.size(); // If this fails, we can likely fix it by forwarding candidacies before votes.
                    // Let's force termination of the SCC.
                    activePorts.forEach(port -> port.sendRequest(new Request.TerminateSCC(forwardedCandidacy)));
                } else {
                    // Forward the vote to our ancestor port
                    sendResponse(treeAncestorPort.owner(), treeAncestorPort, treeVote);
                }
            }
        }
    }

    protected boolean checkTermination() {
        return allPortsDone();
    }

    protected void onTermination() {
        assert allPortsDone() || treeVote != null; // TODO: Bit of a weak assert
        if (!answerTable.isComplete()) {
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Response toSend = answerTable.recordDone();
            subscribers.forEachRemaining(subscriber -> sendResponse(subscriber.owner(), subscriber, toSend));
        }
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
        private Response.TreeVote receivedTreeVote;

        protected Port(ActorNode<?> owner, ActorNode<?> remote) {
            this.owner = owner;
            this.remote = remote;
            this.state = State.READY;
            this.lastRequestedIndex = -1;
            this.receivedCandidacy = null;
        }

        protected void recordReceive(Response msg) {
            assert msg.type() == Response.ResponseType.CANDIDACY || msg.type() == Response.ResponseType.TREE_VOTE  || (state == State.PULLING && lastRequestedIndex == msg.index());
            if (msg.type() == Response.ResponseType.DONE) {
                state = State.DONE;
            } else if (msg.type() == Response.ResponseType.CANDIDACY) {
                this.receivedCandidacy = msg.asCandidacy();
                // Don't stop on pulling on a candidacy
            } else if (msg.type() == Response.ResponseType.TREE_VOTE) {
                receivedTreeVote = msg.asTreeVote();
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
