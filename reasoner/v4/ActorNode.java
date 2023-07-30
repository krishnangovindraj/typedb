package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends Actor<NODE> {

    protected enum State {READY, PULLING, DONE}

    private static final Logger LOG = LoggerFactory.getLogger(ActorNode.class);

    protected final NodeRegistry nodeRegistry;
    protected List<ActorNode.Port> ports;
    private int openAcyclicPorts; // Out of both cyclic and acyclic ports, how many haven't returned ACYCLIC_DONE?
    private int fullyOpenCyclicPorts; // How many cyclic ports are still not DONE?
    private int conditionallyOpenCyclicPorts; // How many cyclic ports are still not DONE?

    // Termination proposal
    private final Integer birthTime;
    private Integer earliestReachableNodeBirth;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(driver, debugName);
        this.nodeRegistry = nodeRegistry;
        this.birthTime = nodeRegistry.nextNodeAge();
        this.ports = new ArrayList<>();
        this.openAcyclicPorts = 0;
        this.fullyOpenCyclicPorts = 0;
        this.conditionallyOpenCyclicPorts = 0;
        this.earliestReachableNodeBirth = birthTime;
    }

    protected void initialise() {

    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    public abstract void readAnswerAt(ActorNode.Port reader, int index);

    public abstract void receive(ActorNode.Port port, Message message);

    protected Port createPort(ActorNode<?> remote) {
        earliestReachableNodeBirth = Math.min(this.earliestReachableNodeBirth, remote.birthTime);
        return createPort(remote, false);
    }

    protected Port createPort(ActorNode<?> remote, boolean isCyclic) {
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
        int messageEarliestReachableNodeBirth = port.remote.earliestReachableNodeBirth; // TODO: Move to message?
        earliestReachableNodeBirth = Math.min(earliestReachableNodeBirth, messageEarliestReachableNodeBirth);
        receive(port, message);
    }

    private void recordConditionallyDone(ActorNode.Port port) {
        if (port.isCyclic) {
            fullyOpenCyclicPorts -= 1; // Else, it's just passing through
            conditionallyOpenCyclicPorts += 1;
        }
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
        private int nextIndex;
        private final boolean isCyclic; // Is the edge potentially a cycle? Only true for edges from concludable to conjunction
        private boolean isConditionallyDone; // This should be in the state

        private Port(ActorNode<?> owner, ActorNode<?> remote, boolean isCyclic) {
            this.owner = owner;
            this.remote = remote;
            this.state = State.READY;
            this.nextIndex = 0;
            this.isCyclic = isCyclic;
            this.isConditionallyDone = false;
        }

        private void recordReceive(Message msg) {
            assert nextIndex == msg.index();
            nextIndex += 1;
            if (msg.type() == Message.MessageType.DONE) {
                state = State.DONE;
                this.isConditionallyDone = true; // incase
                owner.recordDone(this);
            } else if (msg.type() == Message.MessageType.CONDITIONALLY_DONE) {
                this.isConditionallyDone = true;
                owner.recordConditionallyDone(this);
            } else {
                state = State.READY;
            }
        }

        public void readNext() {
            assert state == State.READY;
            state = State.PULLING;
            remote.driver().execute(nodeActor -> nodeActor.readAnswerAt(Port.this, nextIndex));
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

        public int nextIndex() {
            return nextIndex;
        }
    }
}
