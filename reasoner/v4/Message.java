package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.concept.answer.ConceptMap;

import javax.annotation.Nullable;
import java.util.Optional;

public class Message {
    private final MessageType msgType;
    private final int index;
    private final ConceptMap answer;

    public enum MessageType {
        ANSWER,
        DONE,
//        ACYCLIC_DONE ?
    }


    private Message(MessageType msgType, int index, @Nullable ConceptMap answer) {
        assert msgType != MessageType.ANSWER || answer != null;
        this.msgType = msgType;
        this.answer = answer;
        this.index = index;
    }

    public static Message answer(int index, ConceptMap conceptMap) {
        return new Message(MessageType.ANSWER, index, conceptMap);
    }

    public static Message done(int index) {
        return new Message(MessageType.DONE, index, null);
    }

    public MessageType type() {
        return msgType;
    }
    public int index() {
        return index;
    }

    public Optional<ConceptMap> answer() {
        return Optional.ofNullable(answer);
    }
}
