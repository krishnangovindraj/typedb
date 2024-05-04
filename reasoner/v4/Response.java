package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Map;
import java.util.Objects;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class Response {
    private final int index; // TODO: Consider moving down to answer & conclusion

    public Response.Answer asAnswer() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), Answer.class);
    }

    public Response.Conclusion asConclusion() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), Conclusion.class);
    }

    public Candidacy asCandidacy() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), Candidacy.class);
    }

    public enum ResponseType {
        ANSWER,
        CONCLUSION,
        CANDIDACY,
        TERMINATION_PROPOSAL,
        DONE,
    }

    private Response(int index) {
        this.index = index;
    }

    public abstract ResponseType type();

    public int index() {
        return index;
    }

    @Override
    public String toString() {
        return String.format("Msg:[%d: %s]", index, type().name());
    }

    public static class Answer extends Response {

        private final ConceptMap answer;

        public Answer(int index, ConceptMap answer) {
            super(index);
            this.answer = answer;
        }

        public ConceptMap answer() {
            return answer;
        }

        @Override
        public Response.Answer asAnswer() {
            return this;
        }

        @Override
        public ResponseType type() {
            return ResponseType.ANSWER;
        }

        @Override
        public String toString() {
            return String.format("Msg:[%d: %s | %s]", index(), type().name(), answer);
        }
    }

    public static class Conclusion extends Response {

        private final Map<Identifier.Variable, Concept> conclusionAnswer;

        public Conclusion(int index, Map<Identifier.Variable, Concept> conclusionAnswer) {
            super(index);
            this.conclusionAnswer = conclusionAnswer;
        }

        public Map<Identifier.Variable, Concept> conclusionAnswer() {
            return conclusionAnswer;
        }

        public Response.Conclusion asConclusion() {
            return this;
        }

        @Override
        public ResponseType type() {
            return ResponseType.CONCLUSION;
        }
    }

    public static class Done extends Response {

        public Done(int index) {
            super(index);
        }

        @Override
        public ResponseType type() {
            return ResponseType.DONE;
        }
    }

    public static class Candidacy extends Response {
        public final int nodeId;
        public final int tableSize;

        public Candidacy(int nodeId, int tableSize) {
            super(-1);
            this.nodeId = nodeId;
            this.tableSize = tableSize;
        }


        public Candidacy asCandidacy() {
            return this;
        }

        @Override
        public ResponseType type() {
            return ResponseType.CANDIDACY;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.nodeId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Candidacy other = (Candidacy) o;
            return super.equals(other) && nodeId == other.nodeId;
        }
    }
}
