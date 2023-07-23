package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.reasoner.v4.nodes.ConjunctionNode;
import com.vaticle.typedb.core.reasoner.v4.nodes.ResolvableNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class NodeRegistry {
    private final ActorExecutorGroup executorService;
    private final Map<ResolvableConjunction, ConjunctionSubRegistry> conjunctionSubRegistries;
    private final Map<Resolvable<?>, ResolvableSubRegistry> resolvableSubRegistries;

    NodeRegistry(ActorExecutorGroup executorService) {
        this.executorService = executorService;
        this.conjunctionSubRegistries = new ConcurrentHashMap<>();
        this.resolvableSubRegistries = new ConcurrentHashMap<>();
    }

    public ConjunctionSubRegistry conjunctionSubRegistry(ResolvableConjunction conjunction) {
        return conjunctionSubRegistries.computeIfAbsent(conjunction, conj -> new ConjunctionSubRegistry(conj));
    }

    public ResolvableSubRegistry resolvableSubRegistry(Resolvable<?> resolvable) {
        return resolvableSubRegistries.computeIfAbsent(resolvable, res -> new ResolvableSubRegistry(res));
    }

    private <NODE extends ActorNode<NODE>> Actor.Driver<NODE> createDriver(Function<Actor.Driver<NODE>, NODE> actorFn) {
        return Actor.driver(actorFn, executorService);
    }

    public abstract class SubRegistry<KEY, NODE extends ActorNode<NODE>> {
        protected final KEY key;
        private final Map<ConceptMap, NODE> subRegistry;

        private SubRegistry(KEY key) {
            this.key = key;
            this.subRegistry = new ConcurrentHashMap<>();
        }

        public NODE getNode(ConceptMap bounds) {
            return subRegistry.computeIfAbsent(bounds, b -> createNode(b).actor());
        }

        protected abstract Actor.Driver<NODE> createNode(ConceptMap bounds);
    }

    public class ResolvableSubRegistry extends SubRegistry<Resolvable<?>, ResolvableNode> {

        private ResolvableSubRegistry(Resolvable<?> resolvable) {
            super(resolvable);
        }

        @Override
        protected Actor.Driver<ResolvableNode> createNode(ConceptMap bounds) {
            return createDriver(driver -> new ResolvableNode(key, bounds, NodeRegistry.this, driver));
        }
    }

    public class ConjunctionSubRegistry extends SubRegistry<ResolvableConjunction, ConjunctionNode> {

        private ConjunctionSubRegistry(ResolvableConjunction conjunction) {
            super(conjunction);
        }

        @Override
        protected Actor.Driver<ConjunctionNode> createNode(ConceptMap bounds) {
            return createDriver(driver -> new ConjunctionNode(key, bounds, NodeRegistry.this, driver));
        }
    }
}
