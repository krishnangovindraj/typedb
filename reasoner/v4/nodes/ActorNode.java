package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.v4.Request;
import com.vaticle.typedb.core.reasoner.v4.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends AbstractAcyclicNode<NODE> {

    // TODO: See if we can optimise things a bit.
    // We need to implement the non-message optimal tree-based termination algorithm
    // As a small optimisation, we'd like to write the TreeVote message to the answerTable so we force all previous answers to be read by whichever other nodes are in the SCC
    // We need two TreeVote  messages. One for the non-ancestors and one for the ancestors.
    //      The one for non-ancestors is written in the grow-tree phase, and has contribution 0.
    //  The one for ancestors is written in the return phase when we have read such a message from ALL descendants (not just tree descendants) with the actual subtree sum
    // At the start of an iteration, the SCC leader must write these messages to it's table, and then send a growTree to all it's with the new target (0 for the first iteration).
    // The target for further iterations is the subtree sum + the size of the (Careful to include/exclude the TreeVote message)

    static final Logger LOG = LoggerFactory.getLogger(ActorNode.class);

    private final Set<ActorNode.Port> downstreamPorts;
    private Response.Candidacy forwardedCandidacy;
    private Request.GrowTree receivedGrowTree;
    private Port receivedGrowTreeAncestor;
    private Response.TreeVote forwardedTreeVote;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(nodeRegistry, driver, debugName);
        forwardedCandidacy = null;
        downstreamPorts = new HashSet<>();
        receivedGrowTree = new Request.GrowTree(this.nodeId, 0); // TODO: Restoring when state changes weep ;_;
        forwardedTreeVote = null;
    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    @Override
    protected void readAnswerAt(ActorNode.Port reader, Request.ReadAnswer readAnswerRequest) {
        this.downstreamPorts.add(reader); // TODO: Maybe a better way of doing this. ConcurrentHashSet and we add on create?

        int index = readAnswerRequest.index;
        Optional<Response> peekAnswer = answerTable.answerAt(index);
        if (peekAnswer.isPresent()) {
            sendResponse(reader.owner, reader, peekAnswer.get());
        } else if (reader.owner.nodeId >= this.nodeId) {
            sendResponse(reader.owner, reader, new Response.Candidacy(this.nodeId));
            computeNextAnswer(reader, index);
        } else {
            computeNextAnswer(reader, index);
        }
    }

    protected void growTree(ActorNode.Port requestorPort, Request.GrowTree growTreeRequest) {
        if (growTreeRequest.root != forwardedCandidacy.nodeId)  return;
        if (receivedGrowTree == null || growTreeRequest.root != receivedGrowTree.root  || growTreeRequest.target > receivedGrowTree.target ) { // Each iteration can have a different root
            receivedGrowTree = growTreeRequest;
            receivedGrowTreeAncestor = requestorPort;
            // Write the first message (recordPreVoteHere) here  if we optimise
            activePorts.stream().filter(port -> port.receivedCandidacy != null && port.receivedCandidacy.nodeId == receivedGrowTree.root)
                    .forEach(port -> port.sendRequest(growTreeRequest));
        } else {
            sendResponse(requestorPort.owner, requestorPort, new Response.TreeVote(growTreeRequest.root, growTreeRequest.target, 0));
        }
    }

    protected void terminateSCC(ActorNode.Port requestorPort, Request.TerminateSCC terminateSCC) {
        if (requestorPort == receivedGrowTreeAncestor) {
            activePorts.forEach(port -> port.sendRequest(terminateSCC));
            onTermination(); // A negated node can just terminate and let answers stream in later. We do the same.
        }
    }

    // Response handling
    @Override
    protected void handleCandidacy(Port onPort, Response.Candidacy candidacy) {
        checkCandidacyStatusChange(); // We may still have to forward a better tableSize to everyone.
        if (candidacy.nodeId == this.receivedGrowTree.root) {
            // Grow tree on port
            onPort.sendRequest(new Request.GrowTree(this.receivedGrowTree.root, this.receivedGrowTree.target));
        }
    }

    @Override
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
                .min(Comparator.comparing(x -> x.nodeId)); // TODO: Maybe these can be a single field that only needs to be re-evaluated in the case of an upstream termination
        if (oldestCandidate.isEmpty()) return;
        if (forwardedCandidacy == null || !forwardedCandidacy.equals(oldestCandidate.get())) {
            forwardedCandidacy = oldestCandidate.get();
            downstreamPorts.forEach(port -> sendResponse(port.owner, port, forwardedCandidacy));
        }
    }

    private void checkTreeStatusChange() {
        int highestTarget = 0;
        int lowestTarget = Integer.MAX_VALUE;
        int sum = 0;
        for (Port port: activePorts) {
            if (port.receivedTreeVote == null) return;
            assert port.receivedTreeVote.candidate == receivedGrowTree.root;
            highestTarget = Math.max(highestTarget, port.receivedTreeVote.target);
            lowestTarget = Math.min(lowestTarget, port.receivedTreeVote.target);
            sum += port.receivedTreeVote.subtreeContribution;
        }
        if (highestTarget == lowestTarget && (forwardedTreeVote == null || highestTarget > forwardedTreeVote.target)) {
            forwardedTreeVote = new Response.TreeVote(receivedGrowTree.root, highestTarget, sum + answerTable.size());
            if (forwardedTreeVote.candidate == this.nodeId) {
                assert forwardedTreeVote.target == receivedGrowTree.target;
                if (forwardedTreeVote.subtreeContribution == forwardedTreeVote.target) {
                    // Terminate
                    activePorts.forEach(port -> port.sendRequest(new Request.TerminateSCC(forwardedTreeVote)));
                } else {
                    // Start another iteration
                    // If we optimise, write the treePostVote here and below
                    receivedGrowTree = new Request.GrowTree(this.nodeId, forwardedTreeVote.subtreeContribution);
                    activePorts.forEach(port -> port.sendRequest(receivedGrowTree));
                }
            } else {
                // If we optimise, write the treePostVote here and above
                sendResponse(receivedGrowTreeAncestor.owner, receivedGrowTreeAncestor, forwardedTreeVote);
            }
        }
    }

    protected boolean checkTermination() {
        return allPortsDone();
    }

    protected void onTermination() {
        assert allPortsDone() || forwardedTreeVote != null; // TODO: Bit of a weak assert
        if (!answerTable.isComplete()) {
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Response toSend = answerTable.recordDone();
            subscribers.forEachRemaining(subscriber -> sendResponse(subscriber.owner(), subscriber, toSend));
            System.err.printf("TERMINATE: Node[%d] has terminated\n", this.nodeId);
        }
    }


    protected Port createPort(ActorNode<?> remote) {
        System.err.printf("PORT: Node[%d] opened a port to Node[%d]\n", this.nodeId, remote.nodeId);
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
            System.err.printf("SEND_REQUEST: Node[%d] sent request %s to Node[%d]\n", owner.nodeId, request, remote.nodeId);
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
