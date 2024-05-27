package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.v4.Request;
import com.vaticle.typedb.core.reasoner.v4.Response;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class AbstractAcyclicNode<NODE extends AbstractAcyclicNode<NODE>> extends Actor<NODE> {

    protected final NodeRegistry nodeRegistry;
    protected final Integer nodeId;
    protected List<ActorNode.Port> ports;
    protected final Set<ActorNode.Port> activePorts;
    protected final AnswerTable answerTable;

    protected AbstractAcyclicNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(driver, debugName);
        this.nodeRegistry = nodeRegistry;
        nodeId = nodeRegistry.nextNodeAge();
        ports = new ArrayList<>();
        activePorts = new HashSet<>();
        answerTable = new AnswerTable();
    }

    protected void initialise() {
        System.out.printf("CREATION: Node[%d] has been created as a %s\n", this.nodeId, this.getClass().getSimpleName());
    }

    public void receiveRequest(ActorNode.Port requester, Request request) {
        System.out.printf("RECV_REQUEST: Node[%d] received request %s from Node[%d]\n", this.nodeId, request, requester.owner().nodeId);
        switch (request.type()) {
            case HELLO : {
                hello(requester, request.asHello());
                break;
            }
            case READ_ANSWER: {
                readAnswerAt(requester, request.asReadAnswer());
                break;
            }
            case GROW_TREE: {
                growTree(requester, request.asGrowTree());
                break;
            }
            case TERMINATE_SCC: {
                terminateSCC(requester, request.asTerminateSCC());
                break;
            }
        }
    }

    protected void readAnswerAt(ActorNode.Port reader, Request.ReadAnswer readAnswerRequest) {
        readAnswerAtStrictlyAcyclic(reader, readAnswerRequest.index);
    }

    protected abstract void hello(ActorNode.Port proposer, Request.Hello helloRequest);

    protected abstract void growTree(ActorNode.Port proposer, Request.GrowTree growTreeRequest);

    protected abstract void terminateSCC(ActorNode.Port requester, Request.TerminateSCC terminateSCC);

    private void readAnswerAtStrictlyAcyclic(ActorNode.Port reader, int index) {
        Optional<Response> peekAnswer = answerTable.answerAt(index);
        if (peekAnswer.isPresent()) {
            sendResponse(reader.owner(), reader, peekAnswer.get());
        } else {
            computeNextAnswer(reader, index); // This is now effectively a 'pull'
        }
    }

    protected abstract void computeNextAnswer(ActorNode.Port reader, int index);

    public void receiveResponse(ActorNode.Port onPort, Response received) {
        switch (received.type()) {
            case ANSWER: {
                handleAnswer(onPort, received.asAnswer());
                break;
            }
            case CONCLUSION: {
                handleConclusion(onPort, received.asConclusion());
                break;
            }
            case CANDIDACY: {
                handleCandidacy(onPort, received.asCandidacy());
                break;
            }
            case TREE_VOTE: {
                handleTreeVote(onPort, received.asTreeVote());
                break;
            }
            case DONE: {
                recordDone(onPort);
                handleDone(onPort);
                break;
            }
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    protected abstract void handleAnswer(ActorNode.Port onPort, Response.Answer answer);

    protected void handleConclusion(ActorNode.Port onPort, Response.Conclusion conclusion) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected abstract void handleCandidacy(ActorNode.Port onPort, Response.Candidacy candidacy);
    protected abstract void handleTreeVote(ActorNode.Port onPort, Response.TreeVote treeVote);

    protected abstract void handleDone(ActorNode.Port onPort);

    protected abstract ActorNode.Port createPort(ActorNode<?> remote);

    protected FunctionalIterator<ActorNode.Port> allPorts() {
        return Iterators.iterate(ports);
    }

    // TODO: See if i can safely get recipient from port
    protected void sendResponse(ActorNode<?> recipient, ActorNode.Port recipientPort, Response response) { // TODO: Refactor redundant recipient
        System.out.printf("SEND_RESPONSE: Node[%d] sent response %s to Node[%d]\n", this.nodeId, response, recipient.nodeId);
        assert recipientPort.remote().equals(this);
        recipient.driver().execute(actor -> actor.receiveResponseOnPort(recipientPort, response));
    }

    protected void receiveResponseOnPort(ActorNode.Port port, Response response) {
        System.out.printf("RECV_RESPONSE: Node[%d] received response %s from Node[%d]\n", port.owner().nodeId, response, port.remote().nodeId);
        port.mayUpdateStateOnReceive(response); // Is it strange that I call this implicitly?
        receiveResponse(port, response);
    }

    protected void recordDone(ActorNode.Port port) {
        assert activePorts.contains(port);
        activePorts.remove(port);
    }

    protected boolean allPortsDone() { // TODO: Cleanup: Just use activePorts.isEmpty() everywhere
        return activePorts.isEmpty(); // || activePorts.stream().allMatch(port -> port.state() == ActorNode.Port.State.TERMINATING);
    }

    @Override
    protected void exception(Throwable e) {
        nodeRegistry.terminate(e);
    }

    @Override
    public void terminate(Throwable e) {
        super.terminate(e);
    }
}
