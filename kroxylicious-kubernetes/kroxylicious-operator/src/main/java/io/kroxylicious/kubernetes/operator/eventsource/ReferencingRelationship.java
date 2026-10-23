/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.eventsource;

import java.util.function.Predicate;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

/**
 * A {@link Relationship} where the secondary (perhaps transitively) holds the reference back to the primary,
 * as built by {@link Ref#toPrimary} and {@link Ref#toPrimaryVia}. Resolving "which secondaries reference this
 * primary" requires scanning candidate secondaries with a {@link Predicate}; this interface exposes that
 * predicate so {@link Ref#toPrimaryVia} can compose relationships transitively through an intermediate
 * resource. Not exposed outside this package: callers only ever see the {@link Relationship} supertype.
 *
 * @param <P> the type of the primary resource
 * @param <S> the type of the secondary resource
 */
@FunctionalInterface
interface ReferencingRelationship<P extends HasMetadata, S extends HasMetadata> extends Relationship<P, S> {

    /**
     * Returns a predicate selecting the secondaries of type {@code S} that reference the given primary.
     *
     * @param context the event source context
     * @param primary the primary whose referencing secondaries are sought
     * @return a predicate that is {@code true} for secondaries referencing {@code primary}
     */
    Predicate<S> referencingSecondaries(EventSourceContext<P> context, P primary);

    @Override
    default PrimaryToSecondaryMapper<P> primaryToSecondary(EventSourceContext<P> context, Class<S> secondaryType) {
        return new ReferencingSecondaryMapper<>(context, secondaryType, this);
    }

    @Override
    default SecondaryToPrimaryMapper<S> secondaryToPrimary(EventSourceContext<P> context) {
        return new AllPrimariesInNamespaceMapper<P>(context).forSecondary();
    }
}
