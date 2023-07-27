package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concurrent.actor.Actor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public abstract class ActorNode<NODE extends ActorNode<NODE>> extends Actor<NODE> {
    protected enum State {READY, PULLING, DONE}

    protected final NodeRegistry nodeRegistry;

    protected List<ActorNode.Port> ports;
    private int openPortsAcyclic;
    private int openPortsCyclic;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(driver, debugName);
        this.nodeRegistry = nodeRegistry;
        this.ports = new ArrayList<>();
        this.openPortsAcyclic = 0;
        this.openPortsCyclic = 0;
    }

    protected void initialise() {

    }

    // TODO: Since port has the index in it, maybe we don't need index here?
    public abstract void readAnswerAt(ActorNode.Port reader, int index);

    public abstract void receive(ActorNode.Port port, Message message);

    protected Port createPort(ActorNode<?> remote) {
        return createPort(remote, false);
    }

    protected Port createPort(ActorNode<?> remote, boolean isCyclic) {
        Port port = new Port(this, remote, isCyclic);
        ports.add(port);
        if (isCyclic) openPortsCyclic += 1;
        else openPortsAcyclic += 1;
        return port;
    }

    protected boolean allPortsDone() {
        return openPortsAcyclic + openPortsCyclic == 0;
    }

    protected boolean acyclicPortsDone() {
        return openPortsAcyclic == 0;
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
        port.recordReceive(message);
        receive(port, message);
    }

    private void recordDone(ActorNode.Port port) {
        if (port.isCyclic) openPortsCyclic -= 1;
        else openPortsAcyclic -= 1;
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

        private Port(ActorNode<?> owner, ActorNode<?> remote, boolean isCyclic) {
            this.owner = owner;
            this.remote = remote;
            this.state = State.READY;
            this.nextIndex = 0;
            this.isCyclic = isCyclic;
        }

        private void recordReceive(Message msg) {
            assert nextIndex == msg.index();
            nextIndex += 1;
            if (msg.type() == Message.MessageType.DONE) {
                state = State.DONE;
                owner.recordDone(this);
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

        public State state() {
            return state;
        }

        public int nextIndex() {
            return nextIndex;
        }

        public ActorNode<?>  remote() {
            return remote;
        }
    }
}
