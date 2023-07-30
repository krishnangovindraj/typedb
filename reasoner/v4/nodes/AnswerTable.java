package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class AnswerTable {

    private final List<Message> answers;
    private Set<ActorNode.Port> subscribers;
    private boolean complete;
    private boolean conditionallyDone;

    public AnswerTable() {
        this.answers = new ArrayList<>();
        this.subscribers = new HashSet<>();
        this.complete = false;
        this.conditionallyDone = false;
    }

    public int size() {
        return answers.size();
    }

    public boolean isComplete() {
        return complete;
    }

    public boolean isConditionallyDone() {
        return complete || conditionallyDone;
    }

    public Optional<Message> answerAt(int index) {
        assert index < answers.size() || (index == answers.size() && !complete);
        return index < answers.size() ? Optional.of(answers.get(index)) : Optional.empty();
    }

    public void registerSubscriber(ActorNode.Port subscriber, int index) {
        assert index == answers.size() && !complete;
        subscribers.add(subscriber);
    }

    public FunctionalIterator<ActorNode.Port> clearAndReturnSubscribers(int index) {
        assert index == answers.size() && !complete;
        Set<ActorNode.Port> subs = subscribers;
        subscribers = new HashSet<>();
        return Iterators.iterate(subs);
    }

    public Message recordAnswer(ConceptMap answer) {
        assert !complete;
        Message msg = new Message.Answer(answers.size(), answer);
        answers.add(msg);
        return msg;
    }

    public Message recordConclusion(Map<Identifier.Variable, Concept> conclusionAnswer) { // TODO: Generics
        Message msg = new Message.Conclusion(answers.size(), conclusionAnswer);
        answers.add(msg);
        return msg;
    }

    public Message recordDone() {
        assert !complete;
        Message msg = new Message.Done(answers.size());
        answers.add(msg);
        this.complete = true;
        return msg;
    }

    public Message recordAcyclicDone() {
        assert !conditionallyDone;
        Message msg = new Message.ConditionallyDone(answers.size());
        answers.add(msg);
        this.conditionallyDone = true;
        return msg;
    }

    public Message recordTerminationProposal(Message.TerminationProposal terminationProposal) {
        // TODO: Find a way to get away without storing all of these but store just one
        answers.add(terminationProposal);
        return terminationProposal;
    }

    public static class TerminationProposal {

        private final int proposerBirth;
        private final int proposerIndex;

        public TerminationProposal(int proposerBirth, int proposerIndex) {
            this.proposerBirth = proposerBirth;
            this.proposerIndex = proposerIndex;
        }

        public boolean betterThan(TerminationProposal other) {
            return other.proposerBirth < this.proposerBirth ||
                    (other.proposerBirth == this.proposerBirth && other.proposerIndex() > this.proposerIndex());
        }

        public int proposerBirth() {
            return proposerBirth;
        }

        public int proposerIndex() {
            return proposerBirth;
        }
    }
}
