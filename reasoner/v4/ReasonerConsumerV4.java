package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public interface ReasonerConsumerV4 extends ReasonerConsumer<ConceptMap> {

    @Override
    default void setRootProcessor(Actor.Driver<? extends AbstractProcessor<?, ConceptMap, ?, ?>> rootProcessor) {
        throw new RuntimeException(ILLEGAL_STATE.message());
    }

    default void exception(Throwable e) {
        throw new RuntimeException(ILLEGAL_STATE.message());
    }
}
