package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.exception.TypeDBException;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class Request {

    public enum RequestType {
        READ_ANSWER, GROW_TREE, TERMINATE_SCC,
    }

    private Request() { }

    public abstract RequestType type();

    public Request.ReadAnswer asReadAnswer() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), Request.ReadAnswer.class);
    }
    public Request.GrowTree asGrowTree() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), Request.GrowTree.class);
    }

    public TerminateSCC asTerminateSCC() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), Request.TerminateSCC.class);
    }

    @Override
    public abstract String toString();

    public static class ReadAnswer extends Request {

        public final int index;

        public ReadAnswer(int index) {
            super();
            this.index = index;
        }

        @Override
        public RequestType type() {
            return RequestType.READ_ANSWER;
        }

        @Override
        public Request.ReadAnswer asReadAnswer() {
            return this;
        }

        @Override
        public String toString() {
            return String.format("ReadAnswer[%d]", index);
        }
    }

    public static class GrowTree extends Request {
        public final int root;
        public GrowTree(int root) {
            super();
            this.root = root;
        }

        @Override
        public RequestType type() {
            return RequestType.GROW_TREE;
        }

        @Override
        public Request.GrowTree asGrowTree() {
            return this;
        }

        @Override
        public String toString() {
            return String.format("GrowTree[%d]", root);
        }
    }

    public static class TerminateSCC extends Request {
        private final Response.Candidacy leaderState; // TODO: We only really need the root.

        public TerminateSCC(Response.Candidacy leaderState) {
            super();
            this.leaderState = leaderState;
        }

        @Override
        public RequestType type() {
            return RequestType.TERMINATE_SCC;
        }

        @Override
        public TerminateSCC asTerminateSCC() {
            return this;
        }

        @Override
        public String toString() {
            return String.format("Terminate[%s]", leaderState);
        }
    }
}