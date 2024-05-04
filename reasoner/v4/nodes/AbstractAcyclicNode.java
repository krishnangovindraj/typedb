package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.v4.Message;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public abstract class AbstractAcyclicNode<NODE extends AbstractAcyclicNode<NODE>> extends Actor<NODE> {

    protected final NodeRegistry nodeRegistry;
    protected final Integer nodeId;
    protected List<ActorNode.Port> ports;
    protected final Set<ActorNode.Port> activePorts;
    protected final AnswerTable answerTable;
    protected Integer pullerId;

    protected AbstractAcyclicNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(driver, debugName);
        this.nodeRegistry = nodeRegistry;
        nodeId = nodeRegistry.nextNodeAge();
        ports = new ArrayList<>();
        activePorts = new HashSet<>();
        answerTable = new AnswerTable();
        pullerId = null; // THIS LEADS TO DEADLOCKS. WHAT IF YOU PULL FROM A HIGHER NODE OUTSIDE THE CYCLE?
    }

    protected void initialise() {

    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    public void readAnswerAt(ActorNode.Port reader, int index) {
        readAnswerAtStrictlyAcyclic(reader, index);
    }

    private void readAnswerAtStrictlyAcyclic(ActorNode.Port reader, int index) {
        Optional<Message> peekAnswer = answerTable.answerAt(index);
        if (peekAnswer.isPresent()) {
            send(reader.owner(), reader, peekAnswer.get());
        } else {
            propagatePull(reader, index); // This is now effectively a 'pull'
        }
    }

    protected abstract void propagatePull(ActorNode.Port reader, int index);

    public void receive(ActorNode.Port onPort, Message received) {
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
            case TERMINATION_PROPOSAL: {
                throw TypeDBException.of(UNIMPLEMENTED);
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

    protected abstract void handleAnswer(ActorNode.Port onPort, Message.Answer answer);

    protected void handleConclusion(ActorNode.Port onPort, Message.Conclusion conclusion) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected void handleCandidacy(ActorNode.Port onPort, Message.Candidacy candidacy) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected abstract void handleDone(ActorNode.Port onPort);

    protected abstract ActorNode.Port createPort(ActorNode<?> remote);

    protected FunctionalIterator<ActorNode.Port> allPorts() {
        return Iterators.iterate(ports);
    }

    // TODO: See if i can safely get recipient from port
    protected void send(ActorNode<?> recipient, ActorNode.Port recipientPort, Message message) {
        assert recipientPort.remote().equals(this);
        recipient.driver().execute(actor -> actor.receiveOnPort(recipientPort, message));
    }

    protected void receiveOnPort(ActorNode.Port port, Message message) {
        ActorNode.LOG.debug(port.owner() + " received " + message + " from " + port.remote());
        port.recordReceive(message); // Is it strange that I call this implicitly?
        receive(port, message);
    }

    private void recordDone(ActorNode.Port port) {
        assert activePorts.contains(port);
        activePorts.remove(port);
    }

    protected boolean allPortsDone() { // TODO: Cleanup: Just use activePorts.isEmpty() everywhere
        return activePorts.isEmpty();
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
