package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.v4.Response;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class AnswerTable {

    private final List<Response> answers;
    private Set<ActorNode.Port> subscribers;
    private boolean complete;

    public AnswerTable() {
        this.answers = new ArrayList<>();
        this.subscribers = new HashSet<>();
        this.complete = false;
    }

    public int size() {
        return answers.size();
    }

    public boolean isComplete() {
        return complete;
    }

    public Optional<Response> answerAt(int index) {
        assert index < answers.size() || (index == answers.size() && !complete);
        return index < answers.size() ? Optional.of(answers.get(index)) : Optional.empty();
    }

    public void registerSubscriber(ActorNode.Port subscriber, int index) {
        assert index == answers.size() && !complete;
        subscribers.add(subscriber);
    }

    public FunctionalIterator<ActorNode.Port> clearAndReturnSubscribers(int index) {
        assert index == answers.size() && !complete : String.format("%d == %d && !%s", index, answers.size(), complete);
        Set<ActorNode.Port> subs = subscribers;
        subscribers = new HashSet<>();
        return Iterators.iterate(subs);
    }

    public Response.Answer recordAnswer(ConceptMap answer) {
        assert !complete;
        Response.Answer msg = new Response.Answer(answers.size(), answer);
        answers.add(msg);
        return msg;
    }

    public Response.Conclusion recordConclusion(Map<Identifier.Variable, Concept> conclusionAnswer) { // TODO: Generics
        Response.Conclusion msg = new Response.Conclusion(answers.size(), conclusionAnswer);
        answers.add(msg);
        return msg;
    }

//    public Response.TreeVote recordTreePreVote(int target, int nodeId) {
//        assert !complete;
//        Response.TreeVote msg = new Response.TreeVote(answers.size(), target, null, nodeId);
//        answers.add(msg);
//        return msg;
//    }
//
//    public Response.TreeVote recordTreePostVote(int target, int subtreeContribution, int nodeId) {
//        assert !complete;
//        Response.TreeVote msg = new Response.TreeVote(answers.size(), target, subtreeContribution, nodeId);
//        answers.add(msg);
//        return msg;
//    }

    public Response.Done recordDone() {
        assert !complete;
        Response.Done msg = new Response.Done(answers.size());
        answers.add(msg);
        this.complete = true;
        return msg;
    }

    public Response.Done getDoneMessageForNonParent(ActorNode.Port port) {
        subscribers.remove(port);
        return new Response.Done( complete ? answers.size() - 1 : answers.size());
    }
}
