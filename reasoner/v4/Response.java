package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Comparator;
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

    public TreeVote asTreeVote() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), TreeVote.class);
    }

    public enum ResponseType {
        ANSWER,
        CONCLUSION,
        CANDIDACY,
        TREE_VOTE,
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
    public abstract String toString();

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
            return String.format("Answer[%d]", index());
//            return String.format("Answer:[%d: %s | %s]", index(), type().name(), answer);
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

        @Override
        public String toString() {
            return String.format("Conclusion[%d]", index());
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

        @Override
        public String toString() {
            return String.format("Done[%d]", index());
        }
    }

    public static class Candidacy extends Response {
        public static java.util.Comparator<? super Candidacy> Comparator = new Comparator<Candidacy>() {
            @Override
            public int compare(Candidacy c1, Candidacy c2) {
                // Lesser is better
                if (c1.nodeId == c2.nodeId) { // Smaller nodeId is better
                    return Integer.compare(-c1.tableSize, -c2.tableSize); // Bigger is better
                } else return Integer.compare(c1.nodeId, c2.nodeId);
            }
        };
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
            return 0 == Comparator.compare(this, other);
        }

        @Override
        public String toString() {
            return String.format("Candidacy[(%d, %d)]", nodeId, tableSize);
        }
    }

    public static class TreeVote extends Response {

        public final Response.Candidacy voteFor;
        public TreeVote(Candidacy voteFor) {
            super(-1);
            this.voteFor = voteFor;
        }

        @Override
        public ResponseType type() {
            return ResponseType.TREE_VOTE;
        }

        @Override
        public TreeVote asTreeVote() {
            return this;
        }

        public boolean supports(Candidacy candidate) {
            return voteFor.nodeId == candidate.nodeId &&
                    (voteFor.tableSize ==  candidate.tableSize || voteFor.tableSize == Integer.MAX_VALUE);
        }

        @Override
        public String toString() {
            return String.format("TreeVote[%s]", voteFor);
        }
    }
}
