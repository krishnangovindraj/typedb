package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.reasoner.common.Traversal;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.AnswerTable;
import com.vaticle.typedb.core.reasoner.v4.Message;
import com.vaticle.typedb.core.reasoner.v4.NodeRegistry;

import java.util.HashSet;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public abstract class ResolvableNode<RESOLVABLE extends Resolvable<?>, NODE extends ResolvableNode<RESOLVABLE, NODE>>
        extends ActorNode<NODE> {

    protected final RESOLVABLE resolvable;
    protected final ConceptMap bounds;

    public ResolvableNode(RESOLVABLE resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<NODE> driver) {
        super(nodeRegistry, driver, () -> "ResolvableNode[" + resolvable + ", " + bounds + "]");
        this.resolvable = resolvable;
        this.bounds = bounds;
    }

    @Override
    protected void exception(Throwable e) {
        throw TypeDBException.of(UNIMPLEMENTED);
    }


    public static class RetrievableNode extends ResolvableNode<Retrievable, RetrievableNode> {

        private final FunctionalIterator<ConceptMap> traversal;
        private final AnswerTable answerTable;


        public RetrievableNode(Retrievable retrievable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<RetrievableNode> driver) {
            super(retrievable, bounds, nodeRegistry, driver);
            this.traversal = Traversal.traversalIterator(nodeRegistry, retrievable.pattern(), bounds);
            this.answerTable = new AnswerTable();
        }

        @Override
        public void readAnswerAt(ActorNode<?> sender, int index) {
            answerTable.answerAt(index).ifPresentOrElse(
                    answer -> send(sender, answer),
                    () -> send(sender, pullTraversalSynchronous())
            );
        }

        private Message pullTraversalSynchronous() {
            return traversal.hasNext() ?
                    answerTable.recordAnswer(traversal.next()) :
                    answerTable.recordDone();
        }

        @Override
        public void receive(ActorNode<?> sender, Message message) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    public static class ConcludableNode extends ResolvableNode<Concludable, ConcludableNode> {
        // TODO: Non abstract
        private final Set<ConceptMap> seenAnswers;
        public ConcludableNode(Concludable resolvable, ConceptMap bounds, NodeRegistry nodeRegistry, Driver<ConcludableNode> driver) {
            super(resolvable, bounds, nodeRegistry, driver);
            this.seenAnswers = new HashSet<>();
        }

        @Override
        public void readAnswerAt(ActorNode<?> sender, int index) {
            throw TypeDBException.of(UNIMPLEMENTED);
        }

        @Override
        public void receive(ActorNode<?> sender, Message message) {
            throw TypeDBException.of(UNIMPLEMENTED);
        }
    }

}
