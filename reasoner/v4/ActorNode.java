package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concurrent.actor.Actor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public abstract class ActorNode<NODE extends ActorNode<NODE>>  extends Actor<NODE> {
    protected enum State { READY, PULLING, DONE }
    protected final NodeRegistry nodeRegistry;

    List<ActorNode.Port> ports;
    private int donePorts;

    protected ActorNode(NodeRegistry nodeRegistry, Driver<NODE> driver, Supplier<String> debugName) {
        super(driver, debugName);
        this.nodeRegistry = nodeRegistry;
        this.ports = new ArrayList<>();
        this.donePorts = 0;
    }

    public abstract void readAnswerAt(ActorNode.Port reader, int index);

    public abstract void receive(ActorNode<NODE>.Port port, Message message);

    protected Port createPort(ActorNode<?> remote) {
        Port port = new Port(remote);
        ports.add(port);
        return port;
    }

    protected boolean allPortsDone() {
        return donePorts == ports.size();
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

    @Override
    protected void exception(Throwable e) {
        nodeRegistry.terminate(e);
    }

    @Override
    public void terminate(Throwable e) {
        super.terminate(e);
    }


    public class Port {

        private final ActorNode<?> remote;
        private State state;
        private int nextIndex;

        private Port(ActorNode<?> remote) {
            this.remote = remote;
            this.state = State.READY;
            this.nextIndex = 0;
        }

        public void recordReceive(Message msg) {
            assert nextIndex == msg.index();
            nextIndex += 1;
            if (msg.type() == Message.MessageType.DONE) {
                state = State.DONE;
                donePorts += 1;
            } else {
                state = State.READY;
            }
        }

        public void readNext() {
            assert state == State.READY;
            state = State.PULLING;
            remote.driver().execute(nodeActor -> nodeActor.readAnswerAt(Port.this, nextIndex));
        }

        public ActorNode<NODE> owner() {
            return ActorNode.this;
        }

        public State state() {
            return state;
        }

        public int nextIndex() {
            return nextIndex;
        }
    }
}
