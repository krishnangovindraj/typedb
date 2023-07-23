package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.reasoner.v4.nodes.ConjunctionNode;
import com.vaticle.typedb.core.reasoner.v4.nodes.ResolvableNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NodeRegistry {
    private final Map<ResolvableConjunction, ConjunctionSubRegistry> conjunctionSubRegistries;
    private final Map<Resolvable<?>, Map<ConceptMap, ResolvableNode>> resolvableSubRegistries;

     NodeRegistry() {
        this.conjunctionSubRegistries = new ConcurrentHashMap<>();
        this.resolvableSubRegistries = new ConcurrentHashMap<>();
    }

    public ConjunctionNode conjunctionSubRegistry(ResolvableConjunction conjunction) {
        return conjunctionSubRegistries.computeIfAbsent(conjunction, conj -> new ConjunctionSubRegistry(conj));
    }

    public ResolvableNode resolvableSubRegistry(Resolvable<?> resolvable) {
        return resolvableSubRegistries.computeIfAbsent(resolvable, res -> new ResolvableSubRegistry(res));
    }

    public abstract class SubRegistry<KEY, NODE extends ActorNode<NODE>> {
        private final KEY key;
        private final Map<ConceptMap, NODE> subRegistry;
        private SubRegistry(KEY key) {
            this.key = key;
            this.subRegistry = new ConcurrentHashMap<>();
        }

        public NODE getNode(ConceptMap bounds) {
            return subRegistry.computeIfAbsent(bounds, b -> createNode(key, bounds, nextDriver()));
        }

        protected abstract NODE createNode(KEY key, ConceptMap bounds, Actor.Driver<NODE> driver);
    }

    public class ResolvableSubRegistry extends SubRegistry<Resolvable<?>, ResolvableNode> {

        private ResolvableSubRegistry(Resolvable<?> resolvable) {
            super(resolvable);
        }

        @Override
        protected ResolvableNode createNode(Resolvable<?> resolvable, ConceptMap bounds, Actor.Driver<ResolvableNode> driver) {
            return new ResolvableNode(resolvable, bounds, NodeRegistry.this, driver);
        }
    }

    public class ConjunctionSubRegistry extends SubRegistry<ResolvableConjunction, ConjunctionNode> {

        private ConjunctionSubRegistry(ResolvableConjunction conjunction) {
            super(conjunction);
        }

        @Override
        protected ConjunctionNode createNode(ResolvableConjunction conjunction, ConceptMap bounds, Actor.Driver<ConjunctionNode> driver) {
            return new ConjunctionNode(conjunction, bounds, NodeRegistry.this, driver);
        }
    }
}
