package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.concept.answer.ConceptMap;

import javax.annotation.Nullable;
import java.util.Optional;

public class Message {
    private final MessageType msgType;
    private final ConceptMap answer;

    public enum MessageType {
        ANSWER,
        DONE,
//        ACYCLIC_DONE ?
    }


    private Message(MessageType msgType, @Nullable ConceptMap answer) {
        assert msgType != MessageType.ANSWER || answer != null;
        this.msgType = msgType;
        this.answer = answer;
    }

    public static Message answer(ConceptMap conceptMap) {
        return new Message(MessageType.ANSWER, conceptMap);
    }

    public static Message done() {
        return new Message(MessageType.DONE, null);
    }

    public MessageType type() {
        return msgType;
    }

    public Optional<ConceptMap> answer() {
        return Optional.ofNullable(answer);
    }
}
