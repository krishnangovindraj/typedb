package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.exception.TypeDBException;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class Request {

    public enum RequestType {
        READ_ANSWER, GROW_TREE,
    }


    public abstract RequestType type();

    public Request.ReadAnswer asReadAnswer() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), Request.ReadAnswer.class);
    }

    public static class ReadAnswer extends Request {

        public final int index;

        public ReadAnswer(int index) {
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
    }
}