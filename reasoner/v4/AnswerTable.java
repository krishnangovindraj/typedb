package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.answer.ConceptMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class AnswerTable {

    private List<Message> answers;
    private boolean complete;
    private Set<ActorNode<?>> subscribers;

    public AnswerTable() {
        this.answers = new ArrayList<>();
        this.complete = false;
    }

    public Optional<Message> answerAt(int index) {
        assert index < answers.size() || (index == answers.size() && !complete);
        return index < answers.size() ? Optional.of(answers.get(index)) : Optional.empty();
    }

    public void registerSubscriber(ActorNode<?> subscriber, int index) {
        assert index == answers.size() && !complete;
        subscribers.add(subscriber);
    }

    public FunctionalIterator<ActorNode<?>> clearAndReturnSubscribers(int index) {
        assert index == answers.size() && !complete;
        Set<ActorNode<?>> subs = subscribers;
        subscribers = new HashSet<>();
        return Iterators.iterate(subs);
    }

    public Message recordAnswer(ConceptMap answer) {
        assert !complete;
        Message msg = Message.answer(answer);
        answers.add(msg);
        return msg;
    }

    public Message recordDone() {
        assert !complete;
        Message msg = Message.done();
        answers.add(msg);
        this.complete = true;
        return msg;
    }
}
