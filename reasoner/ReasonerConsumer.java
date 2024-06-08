package com.vaticle.typedb.core.reasoner;

import com.vaticle.typedb.core.concept.answer.ConceptMap;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public interface ReasonerConsumer {

    void receiveAnswer(ConceptMap answer);

    void finish();

    default void exception(Throwable e) {
        throw new RuntimeException(ILLEGAL_STATE.message());
    }
}
