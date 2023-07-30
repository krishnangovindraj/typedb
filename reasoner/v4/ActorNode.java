package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.v4.nodes.AnswerTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends Actor<NODE> {

    protected enum State {READY, PULLING, DONE}

    private static final Logger LOG = LoggerFactory.getLogger(ActorNode.class);

    protected final NodeRegistry nodeRegistry;
    protected List<ActorNode.Port> ports;
    private int openAcyclicPorts; // Out of both cyclic and acyclic ports, how many haven't returned ACYCLIC_DONE?
    private int fullyOpenCyclicPorts; // How many cyclic ports are still not DONE?
    private int conditionallyOpenCyclicPorts; // How many cyclic ports are still not DONE?

    // Termination proposal
    protected final Integer birthTime;
    protected Integer earliestReachableCyclicNodeBirth;

    protected BestTerminationProposalTracker bestTerminationProposalTracker;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(driver, debugName);
        this.nodeRegistry = nodeRegistry;
        this.birthTime = nodeRegistry.nextNodeAge();
        this.ports = new ArrayList<>();
        this.openAcyclicPorts = 0;
        this.fullyOpenCyclicPorts = 0;
        this.conditionallyOpenCyclicPorts = 0;
        this.earliestReachableCyclicNodeBirth = birthTime;
        this.bestTerminationProposalTracker = new BestTerminationProposalTracker();
    }

    protected void initialise() {

    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    public abstract void readAnswerAt(ActorNode.Port reader, int index);



    public void receive(Port onPort, Message received) {
        switch (received.type()) {
            case ANSWER: {
                handleAnswer(onPort, received.asAnswer());
                break;
            }
            case CONCLUSION: {
                handleConclusion(onPort, received.asConclusion());
                break;
            }
            case CONDITIONALLY_DONE: {
                handleConditionallyDone(onPort);
                break;
            }
            case TERMINATION_PROPOSAL: {
                handleTerminationProposal(onPort, received.asTerminationProposal());
                break;
            }
            case DONE: {
                handleDone(onPort);
                break;
            }
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private void handleTerminationProposal(Port onPort, Message.TerminationProposal terminationProposalMsg) {
        if (!onPort.isCyclic) {
            if (onPort.state() == State.READY) onPort.readNext();
            return; // Not relevant to us
        }

        AnswerTable.TerminationProposal terminationProposal = terminationProposalMsg.terminationProposal();
        if (bestTerminationProposalTracker.allInSupport() && bestTerminationProposalTracker.bestTerminationProposal.proposerBirth() <= this.earliestReachableCyclicNodeBirth) {
            assert terminationProposal.equals(bestTerminationProposalTracker.bestTerminationProposal);
            handleUnanimousTerminationProposal(terminationProposal);
        }
        if (onPort.state() == State.READY) onPort.readNext();
    }

    protected void handleUnanimousTerminationProposal(AnswerTable.TerminationProposal terminationProposal) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected abstract void handleAnswer(Port onPort, Message.Answer answer);

    protected void handleConclusion(Port onPort, Message.Conclusion conclusion) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected void handleConditionallyDone(Port onPort) {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    protected abstract void handleDone(Port onPort);


    protected Port createPort(ActorNode<?> remote) {
        return createPort(remote, false);
    }

    protected Port createPort(ActorNode<?> remote, boolean isCyclic) {
        if (isCyclic) {
            earliestReachableCyclicNodeBirth = Math.min(this.earliestReachableCyclicNodeBirth, remote.birthTime);
        }
        Port port = new Port(this, remote, isCyclic);
        ports.add(port);
        if (isCyclic) fullyOpenCyclicPorts += 1;
        else openAcyclicPorts += 1;
        return port;
    }

    protected boolean allPortsDone() {
        return openAcyclicPorts + fullyOpenCyclicPorts + conditionallyOpenCyclicPorts  == 0;
    }

    protected boolean acyclicPortsDone() { // Condition for conclusions?
        return openAcyclicPorts == 0;
    }

    protected boolean allPortsDoneConditionally() {  // Condition for concludables?
        return openAcyclicPorts + fullyOpenCyclicPorts == 0;
    }

    protected FunctionalIterator<ActorNode.Port> allPorts() {
        return Iterators.iterate(ports);
    }

    // TODO: See if i can safely get recipient from port
    public void send(ActorNode<?> recipient, ActorNode.Port recipientPort, Message message) {
        assert recipientPort.remote == this;
        recipient.driver().execute(actor -> actor.receiveOnPort(recipientPort, message));
    }

    protected void receiveOnPort(Port port, Message message) {
        LOG.debug(port.owner() + " received " + message + " from " + port.remote());
        port.recordReceive(message); // Is it strange that I call this implicitly?
        if (port.isCyclic) {
            int messageEarliestReachableNodeBirth = port.remote.earliestReachableCyclicNodeBirth; // TODO: Move to message?
            earliestReachableCyclicNodeBirth = Math.min(earliestReachableCyclicNodeBirth, messageEarliestReachableNodeBirth);
        }
        receive(port, message);
    }

    private void recordConditionallyDone(ActorNode.Port port) {
        if (port.isCyclic) {
            fullyOpenCyclicPorts -= 1; // Else, it's just passing through
            conditionallyOpenCyclicPorts += 1;
        }
    }

    private void recordTerminationProposal(AnswerTable.TerminationProposal terminationProposal) {
        this.bestTerminationProposalTracker.update(terminationProposal);
    }

    private void recordDone(ActorNode.Port port) {
        if (port.isCyclic) {
            if (!port.isConditionallyDone) recordConditionallyDone(port);
            conditionallyOpenCyclicPorts -= 1;
        }
        else openAcyclicPorts -= 1;
    }

    @Override
    protected void exception(Throwable e) {
        nodeRegistry.terminate(e);
    }

    @Override
    public void terminate(Throwable e) {
        super.terminate(e);
    }

    public static class Port {
        private final ActorNode<?> owner;
        private final ActorNode<?> remote;
        private State state;
        private int lastRequestedIndex;
        private final boolean isCyclic; // Is the edge potentially a cycle? Only true for edges from concludable to conjunction
        private boolean isConditionallyDone; // This should be in the state
        private AnswerTable.TerminationProposal terminationProposal;

        private Port(ActorNode<?> owner, ActorNode<?> remote, boolean isCyclic) {
            this.owner = owner;
            this.remote = remote;
            this.state = State.READY;
            this.lastRequestedIndex = -1;
            this.isCyclic = isCyclic;
            this.isConditionallyDone = false;
            this.terminationProposal = null;
        }

        private void recordReceive(Message msg) {
            assert state == State.PULLING;
            assert lastRequestedIndex == msg.index();
            if (msg.type() == Message.MessageType.DONE) {
                state = State.DONE;
                this.isConditionallyDone = true; // incase
                owner.recordDone(this);
            } else if (msg.type() == Message.MessageType.CONDITIONALLY_DONE) {
                this.isConditionallyDone = true;
                owner.recordConditionallyDone(this);
                state = State.READY;
            } else if (msg.type() == Message.MessageType.TERMINATION_PROPOSAL) {
                assert (this.terminationProposal == null) || (msg.asTerminationProposal().terminationProposal().betterThan(this.terminationProposal));
                this.terminationProposal = msg.asTerminationProposal().terminationProposal();
                owner.recordTerminationProposal(terminationProposal);
                state = State.READY;
            } else {
                state = State.READY;
            }
            assert state != State.PULLING;
        }

        public void readNext() {
            assert state == State.READY;
            state = State.PULLING;
            lastRequestedIndex += 1;
            int readIndex = lastRequestedIndex;
            remote.driver().execute(nodeActor -> nodeActor.readAnswerAt(Port.this, readIndex));
        }

        public ActorNode<?> owner() {
            return owner;
        }

        public ActorNode<?>  remote() {
            return remote;
        }

        public boolean isCyclic() {
            return isCyclic;
        }

        public State state() {
            return state;
        }

        public int lastRequestedIndex() {
            return lastRequestedIndex;
        }
    }

    private class BestTerminationProposalTracker {
        private AnswerTable.TerminationProposal bestTerminationProposal;
        private int countInSupport;

        private BestTerminationProposalTracker() {
            bestTerminationProposal = null;
            countInSupport = 0;
        }

        private void update(AnswerTable.TerminationProposal newTerminationProposal) {
            if (bestTerminationProposal == null) {
                bestTerminationProposal = newTerminationProposal;
                this.countInSupport = 1;
            } else if (newTerminationProposal.equals(this.bestTerminationProposal)) {
                countInSupport += 1;
            } else if (newTerminationProposal.betterThan(this.bestTerminationProposal)) {
                this.bestTerminationProposal = newTerminationProposal;
                this.countInSupport = 1;
            }
        }

        private boolean allInSupport() {
            return ActorNode.this.allPortsDoneConditionally() && this.countInSupport == ActorNode.this.conditionallyOpenCyclicPorts;
        }
    }
}
