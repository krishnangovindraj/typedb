package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.answer.ConceptMap;

import javax.annotation.Nullable;
import java.util.Optional;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public class Message {
    private final MessageType msgType;
    private final int index;

    public Message.Answer asAnswer() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), Answer.class);
    }

    public enum MessageType {
        ANSWER,
        DONE,
//        ACYCLIC_DONE ?
    }


    private Message(MessageType msgType, int index) {
        this.msgType = msgType;
        this.index = index;
    }

    public static Message.Answer answer(int index, ConceptMap conceptMap) {
        return new Message.Answer(MessageType.ANSWER, index, conceptMap);
    }

    public static Message done(int index) {
        return new Message(MessageType.DONE, index);
    }

    public MessageType type() {
        return msgType;
    }

    public int index() {
        return index;
    }

    @Override
    public String toString() {
        return String.format("Msg:[%d: %s]", index, msgType.name());
    }

    public static class Answer extends Message {

        private final ConceptMap answer;

        private Answer(MessageType msgType, int index, ConceptMap answer) {
            super(msgType, index);
            this.answer = answer;
        }

        public ConceptMap answer() {
            return answer;
        }

        @Override
        public Message.Answer asAnswer() {
            return this;
        }

        @Override
        public String toString() {
            return String.format("Msg:[%d: %s | %s]", index(), type().name(), answer);
        }
    }
}
