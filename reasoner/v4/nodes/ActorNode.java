package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.v4.Request;
import com.vaticle.typedb.core.reasoner.v4.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;
//WERE STRUGGLING ON CANDIDACY STABILITY (A LACK OF IT OR TOO MUCH)

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
        downstreamPorts = new HashSet<>();
        forwardedCandidacy = new Response.Candidacy(this.nodeId);
        forwardedTreeVote = null;
        receivedGrowTree = new Request.GrowTree(this.nodeId, 0);
    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    @Override
    protected void readAnswerAt(ActorNode.Port reader, Request.ReadAnswer readAnswerRequest) {
        int index = readAnswerRequest.index;
        Optional<Response> peekAnswer = answerTable.answerAt(index);
        if (peekAnswer.isPresent()) {
            sendResponse(reader.owner, reader, peekAnswer.get());
        } else if (reader.owner.nodeId >= this.nodeId) {
            sendResponse(reader.owner, reader, this.forwardedCandidacy);
            computeNextAnswer(reader, index);
        } else {
            computeNextAnswer(reader, index);
        }
    }

    @Override
    protected void hello(ActorNode.Port onPort, Request.Hello helloRequest) {
        this.downstreamPorts.add(onPort); // TODO: Maybe a better way of doing this. ConcurrentHashSet and we add on create?
        sendResponse(onPort.owner(), onPort, forwardedCandidacy);
    }

    protected void growTree(ActorNode.Port requestorPort, Request.GrowTree growTreeRequest) {
        assert receivedGrowTree != null;
        if (growTreeRequest.root != forwardedCandidacy.nodeId)  return;
        if (growTreeRequest.root != receivedGrowTree.root  || growTreeRequest.target > receivedGrowTree.target ) { // Each iteration can have a different root
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
        // TODO: Add assert that receivedCandidacy is never null
        // Response.Candidacy existingPortCandidacy = (onPort.receivedCandidacy != null ? onPort.receivedCandidacy : NULL_CANDIDACY);
        assert(onPort.receivedCandidacy != null);
        Response.Candidacy existingPortCandidacy = onPort.receivedCandidacy;

        if (existingPortCandidacy.nodeId < candidacy.nodeId) {
            // Happens in the case of termination.
            if (existingPortCandidacy.nodeId == forwardedCandidacy.nodeId) {
                activePorts.forEach(p -> {
                    if (p.receivedCandidacy.nodeId == existingPortCandidacy.nodeId) {
                        p.receivedCandidacy = Port.NULL_RECEIVED_CANDIDACY;
                    }
                });
                forwardedCandidacy = recomputeOldestCandidate();
                downstreamPorts.forEach(port -> sendResponse(port.owner, port, forwardedCandidacy));
            }
        }

        if (candidacy.nodeId < forwardedCandidacy.nodeId) {
            downstreamPorts.forEach(port -> sendResponse(port.owner, port, candidacy));
            forwardedCandidacy = candidacy;
        }

        if (candidacy.nodeId == receivedGrowTree.root) {
            // TODO: If (receivedGrowTree.root != forwardedCandidacy.nodeId), we can avoid this message.
            onPort.sendRequest(new Request.GrowTree(this.receivedGrowTree.root, this.receivedGrowTree.target));
        }

        // Update
        onPort.receivedCandidacy = candidacy;
    }


    @Override
    protected void handleTreeVote(Port onPort, Response.TreeVote treeVote) {
        checkTreeStatusChange();
    }

    @Override
    protected void handleDone(Port onPort) {
        if (allPortsDone()) {
            onTermination();
        } else {
            if (onPort.receivedCandidacy.nodeId == forwardedCandidacy.nodeId) {
                // TODO: Make this better
                activePorts.forEach(p -> {
                    if (p.receivedCandidacy.nodeId == onPort.receivedCandidacy.nodeId) {
                        p.receivedCandidacy = Port.NULL_RECEIVED_CANDIDACY;
                    }
                });
                forwardedCandidacy = recomputeOldestCandidate();
                downstreamPorts.forEach(port -> sendResponse(port.owner, port, forwardedCandidacy));
            }
            checkTreeStatusChange();
        }
    }

    private Response.Candidacy recomputeOldestCandidate() {
        return Stream.concat(
                    Stream.of(new Response.Candidacy(this.nodeId)),
                        activePorts.stream().map(port -> port.receivedCandidacy).filter(Objects::nonNull)
                ).min(Comparator.comparing(x -> x.nodeId)).get();
    }

// TODO: Replace this with the single efficient tracker
//    protected void checkCandidacyStatusChange() {
//        Optional<Response.Candidacy> oldestCandidate = activePorts.stream().map(port -> port.receivedCandidacy).filter(Objects::nonNull) // Ok to filter here, but not for votes.
//                .min(Comparator.comparing(x -> x.nodeId)); // TODO: Maybe these can be a single field that only needs to be re-evaluated in the case of an upstream termination
//        if (oldestCandidate.isEmpty()) return;
////        TODO: Fix the oscillating candidacy. Monotonicity holds until that port terminates.
//        if (forwardedCandidacy == null || !forwardedCandidacy.equals(oldestCandidate.get())) {
//            forwardedCandidacy = oldestCandidate.get();
//            downstreamPorts.forEach(port -> sendResponse(port.owner, port, forwardedCandidacy));
//        }
//    }

    private void checkTreeStatusChange() {
        // if (receivedGrowTree.target == forwardedTreeVote.target) return; // already voted. // TODO: Reenable

        int highestTarget = 0;
        int lowestTarget = Integer.MAX_VALUE;
        int sum = 0;
        for (Port port: activePorts) {
            if (port.receivedTreeVote == null) return;
            if (port.receivedTreeVote.candidate != receivedGrowTree.root ){
                System.out.printf("DEBUG: Node[%d] received treeVote for root: %d but expected root: %d \n", this.nodeId, port.receivedTreeVote.candidate, receivedGrowTree.root);
                return;
            }
            if (port.receivedCandidacy.nodeId != receivedGrowTree.root ){
                System.out.printf("DEBUG: Node[%d] received candidacy for candidate: %d but expected root: %d \n", this.nodeId, port.receivedCandidacy.nodeId, receivedGrowTree.root);
                return;
            }
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
                    System.out.printf("ITERATE: Node[%d] updated target from %d to %d\n", this.nodeId, forwardedTreeVote.target, forwardedTreeVote.subtreeContribution);
                    // If we optimise, write the treePostVote here and below
                    receivedGrowTree = new Request.GrowTree(this.nodeId, forwardedTreeVote.subtreeContribution);
                    assert ports.stream().allMatch(port -> port.receivedCandidacy.nodeId == this.nodeId);
                    activePorts.forEach(port -> port.sendRequest(receivedGrowTree));
                }
            } else {
                // If we optimise, write the treePostVote here and above
                sendResponse(receivedGrowTreeAncestor.owner, receivedGrowTreeAncestor, forwardedTreeVote);
            }
        }
    }

    protected void onTermination() {
        assert allPortsDone() || forwardedTreeVote != null; // TODO: Bit of a weak assert
        if (!answerTable.isComplete()) {
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Response toSend = answerTable.recordDone();
            subscribers.forEachRemaining(subscriber -> sendResponse(subscriber.owner(), subscriber, toSend));
            System.out.printf("TERMINATE: Node[%d] has terminated\n", this.nodeId);
        }
    }


    protected Port createPort(ActorNode<?> remote) {
        System.out.printf("PORT: Node[%d] opened a port to Node[%d]\n", this.nodeId, remote.nodeId);
        Port port = new Port(this, remote);
        ports.add(port);
        activePorts.add(port);
        port.sendRequest(new Request.Hello());
        return port;
    }

    public static class Port {
        private static final Response.Candidacy NULL_RECEIVED_CANDIDACY = new Response.Candidacy(Integer.MAX_VALUE);

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
            this.receivedCandidacy = NULL_RECEIVED_CANDIDACY;
        }

        protected void mayUpdateStateOnReceive(Response msg) {
            assert msg.type() == Response.ResponseType.CANDIDACY || msg.type() == Response.ResponseType.TREE_VOTE  || (state == State.PULLING && lastRequestedIndex == msg.index());
            if (msg.type() == Response.ResponseType.DONE) {
                state = State.DONE;
            } else if (msg.type() == Response.ResponseType.CANDIDACY) {
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
            System.out.printf("SEND_REQUEST: Node[%d] sent request %s to Node[%d]\n", owner.nodeId, request, remote.nodeId);
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
