package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.collection.Pair;
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

    TerminationTracker terminationTracker;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(nodeRegistry, driver, debugName);
        downstreamPorts = new HashSet<>();
        terminationTracker = new TerminationTracker(this.nodeId);
    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    @Override
    protected void readAnswerAt(ActorNode.Port reader, Request.ReadAnswer readAnswerRequest) {
        int index = readAnswerRequest.index;
        Optional<Response> peekAnswer = answerTable.answerAt(index);
        if (peekAnswer.isPresent()) {
            sendResponse(reader.owner, reader, peekAnswer.get());
        } else if (reader.owner.nodeId >= this.nodeId) {
            sendResponse(reader.owner, reader, terminationTracker.currentCandidate);
            computeNextAnswer(reader, index);
        } else {
            computeNextAnswer(reader, index);
        }
    }

    @Override
    protected void hello(ActorNode.Port onPort, Request.Hello helloRequest) {
        this.downstreamPorts.add(onPort); // TODO: Maybe a better way of doing this. ConcurrentHashSet and we add on create?
        sendResponse(onPort.owner(), onPort, terminationTracker.currentCandidate);
    }

    protected void growTree(ActorNode.Port requestorPort, Request.GrowTree growTreeRequest) {
        if (terminationTracker.mustIgnoreGrowTreeRequest(growTreeRequest))  return;
        if (terminationTracker.trySetGrowTreeAncestor(requestorPort, growTreeRequest)) {
            // Write the first message (recordPreVoteHere) here  if we optimise
            activePorts.stream()
                    .filter(port -> port.receivedCandidacy != null && port.receivedCandidacy.nodeId == terminationTracker.currentGrowTreeRequest.first().root)
                    .forEach(port -> port.sendRequest(growTreeRequest));
        } else if (terminationTracker.mustRespondWithEmptyTreeVote(requestorPort, growTreeRequest)) {
            sendResponse(requestorPort.owner, requestorPort, new Response.TreeVote(growTreeRequest.root, growTreeRequest.target, 0));
        }
    }

    protected void terminateSCC(ActorNode.Port requestorPort, Request.TerminateSCC terminateSCC) {
        if (terminationTracker.isTerminateSCCFromAncestor(requestorPort, terminateSCC) ) { // Else we wait for ancestor
            activePorts.forEach(port -> port.sendRequest(terminateSCC));
        } else {
            sendResponse(requestorPort.owner, requestorPort, answerTable.getDoneMessageForNonParent(requestorPort));
        }
    }

    // Response handling
    @Override
    protected void handleCandidacy(Port onPort, Response.Candidacy candidacy) {
        // TODO: Add assert that receivedCandidacy is never null
        // Response.Candidacy existingPortCandidacy = (onPort.receivedCandidacy != null ? onPort.receivedCandidacy : NULL_CANDIDACY);
        assert(onPort.receivedCandidacy != null);
        Response.Candidacy existingPortCandidacy = onPort.receivedCandidacy;
        onPort.receivedCandidacy = candidacy;

        // TODO: Add explicit efficient handling of an increase in candidate on a port
        Optional<Response.Candidacy> updatedCandidate = terminationTracker.mayUpdateCurrentCandidate(existingPortCandidacy, candidacy, activePorts);
        if (updatedCandidate.isPresent()) {
            downstreamPorts.forEach(port -> sendResponse(port.owner, port, candidacy));
        }

        if (candidacy.nodeId == terminationTracker.currentGrowTreeRequest.first().root) {
            // TODO: If (receivedGrowTree.root != forwardedCandidacy.nodeId), we can avoid this message.
            onPort.sendRequest(terminationTracker.currentGrowTreeRequest.first());
        }
    }


    @Override
    protected void handleTreeVote(Port onPort, Response.TreeVote treeVote) {
        onPort.receivedTreeVote = treeVote;
        checkTreeStatusChange();
    }

    private void checkTreeStatusChange() {
        Optional<Response.TreeVote> ourVoteOpt = terminationTracker.mayVote(activePorts);
        if (ourVoteOpt.isEmpty()) return;
        Response.TreeVote ourVote = ourVoteOpt.get();
        if (ourVote.candidate == this.nodeId) {
            // We must either terminate or iterate
            Either<Request.TerminateSCC, Request.GrowTree> terminateOrIterate = terminationTracker.terminateOrIterate(ourVote);
            if (terminateOrIterate.isFirst()) {
                Request.TerminateSCC terminateRequest = terminateOrIterate.first();
                activePorts.forEach(port -> port.sendRequest(terminateRequest));
            } else {
                Request.GrowTree iterateRequest = terminateOrIterate.second();
                activePorts.forEach(port -> port.sendRequest(iterateRequest));
            }
        } else {
            // We must forward the vote to our ancestor
            sendResponse(terminationTracker.growTreeAncestor().owner, terminationTracker.growTreeAncestor(), ourVote);
        }
    }

    @Override
    protected void handleDone(Port onPort) {
        if (allPortsDone()) {
            onTermination();
        } else {
            if (onPort.receivedCandidacy.nodeId == terminationTracker.currentCandidate.nodeId) {
                assert nodeRegistry.isCandidateTerminated(onPort.receivedCandidacy.nodeId);
                Optional<Response.Candidacy> updatedCandidacy = terminationTracker.mayUpdateCurrentCandidate(onPort.receivedCandidacy, new Response.Candidacy(this.nodeId), activePorts); // I really should stop doing hacks like this passing this as the new candidate
                assert updatedCandidacy.isPresent();
                downstreamPorts.forEach(port -> sendResponse(port.owner, port, updatedCandidacy.get()));
            }
            checkTreeStatusChange();
        }
    }

    protected void onTermination() {
        assert allPortsDone(); // TODO: Bit of a weak assert. // Why? this.getClass().equals(NegatedNode.class) ?
        if (!answerTable.isComplete()) {
            FunctionalIterator<Port> subscribers = answerTable.clearAndReturnSubscribers(answerTable.size());
            Response toSend = answerTable.recordDone();
            nodeRegistry.notiyNodeTermination(this.nodeId);
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

        public enum State {READY, PULLING, DONE} // TODO: Do we want a terminating state?
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
            assert msg.type() == Response.ResponseType.CANDIDACY || msg.type() == Response.ResponseType.TREE_VOTE  || ( (state == State.PULLING || state == State.DONE) && lastRequestedIndex == msg.index());
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

    private class TerminationTracker {
        Response.Candidacy currentCandidate;
        Pair<Request.GrowTree, Port> currentGrowTreeRequest; // We can't use nodeId for ancestor because we could have multiple ports

        Response.TreeVote __DBG__lastTreeVote;

        public TerminationTracker(Integer thisNodeId) {
            currentCandidate = new Response.Candidacy(thisNodeId);
            currentGrowTreeRequest = new Pair<>(new Request.GrowTree(thisNodeId, 0), null);
            __DBG__lastTreeVote = null;
        }


        public Port growTreeAncestor() {
            return currentGrowTreeRequest.second();
        }

        public boolean mustIgnoreGrowTreeRequest(Request.GrowTree newGrowTreeRequest) {
            return newGrowTreeRequest.root != terminationTracker.currentCandidate.nodeId;
        }

        public boolean trySetGrowTreeAncestor(Port requestorPort, Request.GrowTree newGrowTreeRequest) {
            assert newGrowTreeRequest.root == currentCandidate.nodeId;
            if (currentGrowTreeRequest.first().root != newGrowTreeRequest.root || newGrowTreeRequest.target > currentGrowTreeRequest.first().target) {
                currentGrowTreeRequest = new Pair<>(newGrowTreeRequest, requestorPort);
                return true;
            } else {
                return false;
            }
        }

        public boolean mustRespondWithEmptyTreeVote(Port requestorPort, Request.GrowTree newGrowTreeRequest) {
            assert requestorPort != currentGrowTreeRequest.second();
            return requestorPort != currentGrowTreeRequest.second() &&
                    newGrowTreeRequest.root == currentGrowTreeRequest.first().root &&
                    newGrowTreeRequest.target == currentGrowTreeRequest.first().target;
        }

        public boolean isTerminateSCCFromAncestor(Port requestorPort, Request.TerminateSCC terminateSCC) {
            assert terminateSCC.sccState().candidate == currentCandidate.nodeId || currentCandidate.nodeId == ActorNode.this.nodeId; // Race condition resets candidate to this node.
            assert terminateSCC.sccState().candidate == currentGrowTreeRequest.first().root &&
                    terminateSCC.sccState().target == currentGrowTreeRequest.first().target;
            return requestorPort == currentGrowTreeRequest.second();
        }

        public Optional<Response.Candidacy> mayUpdateCurrentCandidate(Response.Candidacy existingPortCandidacy, Response.Candidacy candidacy, Set<Port> activePorts) {
            if (existingPortCandidacy.nodeId < candidacy.nodeId) {
                assert nodeRegistry.isCandidateTerminated(existingPortCandidacy.nodeId);
                if (existingPortCandidacy.nodeId == currentCandidate.nodeId) {
                    currentCandidate = selectReplacementCandidate(activePorts);
                    return Optional.of(currentCandidate);
                }
            }

            if (candidacy.nodeId < currentCandidate.nodeId) {
                // Simple replacement
                currentCandidate = candidacy;
                return Optional.of(candidacy);
            } else {
                return Optional.empty();
            }
        }

        private Response.Candidacy selectReplacementCandidate(Set<Port>  activePorts) {
            // TODO: Optimise so we can avoid the isCandidateTerminated check.
            Stream<Response.Candidacy> activePortCandidateStream = activePorts.stream().map(port -> port.receivedCandidacy)
                    .filter(portCandidacy -> portCandidacy != null && !nodeRegistry.isCandidateTerminated(portCandidacy.nodeId));
            return Stream.concat(
                    Stream.of(new Response.Candidacy(ActorNode.this.nodeId)),
                    activePortCandidateStream
            ).min(Comparator.comparing(x -> x.nodeId)).get();
        }

        public Optional<Response.TreeVote> mayVote(Set<Port> activePorts) {
            if (currentGrowTreeRequest.first().root != this.currentCandidate.nodeId) return Optional.empty();
            int subtreeSum = answerTable.size();
            for (Port port: activePorts) {
                if (port.receivedTreeVote == null ||
                        port.receivedTreeVote.candidate != this.currentGrowTreeRequest.first().root ||
                        port.receivedTreeVote.target != this.currentGrowTreeRequest.first().target // Can this happen if new nodes are spawned between iterations due to inflight messages?
                ) {
                    return Optional.empty();
                } else {
                    subtreeSum += port.receivedTreeVote.subtreeContribution;
                }
            }
            Response.TreeVote vote = new Response.TreeVote(this.currentGrowTreeRequest.first().root, this.currentGrowTreeRequest.first().target, subtreeSum);
            __DBG__lastTreeVote = vote;
            return Optional.of(vote);
        }

        public Either<Request.TerminateSCC, Request.GrowTree> terminateOrIterate(Response.TreeVote ourVote) {
            assert ourVote.candidate == ActorNode.this.nodeId;
            if (ourVote.target == this.currentGrowTreeRequest.first().target) {
                return Either.first(new Request.TerminateSCC(ourVote));
            } else {
                currentGrowTreeRequest = new Pair<>(new Request.GrowTree(ActorNode.this.nodeId, ourVote.subtreeContribution), null);
                return Either.second(currentGrowTreeRequest.first());
            }
        }
    }
}
