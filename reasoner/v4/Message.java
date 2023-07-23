package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.concept.answer.ConceptMap;

import javax.annotation.Nullable;
import java.util.Optional;

public abstract class Message {
    private final ConceptMap answer;
    private final MessageType msgType;

    public enum MessageType {
        ANSWER,
        DONE,
//        ACYCLIC_DONE ?
    }
    private final ActorNode<?> sender;


    public Message(ActorNode<?> sender, MessageType msgType, @Nullable ConceptMap answer) {
        this.sender = sender;
        assert msgType != MessageType.ANSWER || answer != null;
        this.msgType = msgType;
        this.answer = answer;
    }

    public MessageType type() {
        return msgType;
    }

    public Optional<ConceptMap> answer() {
        return Optional.ofNullable(answer);
    }
}
