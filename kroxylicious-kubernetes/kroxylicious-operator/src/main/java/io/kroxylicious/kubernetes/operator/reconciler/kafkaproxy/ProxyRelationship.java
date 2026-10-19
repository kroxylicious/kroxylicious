/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.function.Predicate;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

/**
 * Describes how a secondary resource of type {@code S} is related to a {@link KafkaProxy}, answering the
 * question "which secondaries reference this proxy?". Used by {@link ReferencingSecondaryMapper} to map a
 * changed proxy to the secondary resources that should be reconciled as a result.
 * <p>
 * Rather than a flat {@code (secondary, proxy) -> boolean}, the relationship is resolved in two steps for a
 * fixed proxy: {@link #referencingSecondaries} may perform any listing/precomputation once (e.g. a transitive
 * relationship lists its intermediate resources a single time), returning a cheap {@link Predicate} evaluated
 * once per candidate secondary.
 *
 * @param <S> the type of the secondary resource
 * @see Ref for the factory methods ({@link Ref#direct}, {@link Ref#via}) that build these.
 */
@FunctionalInterface
interface ProxyRelationship<S extends HasMetadata> {

    /**
     * Returns a predicate selecting the secondaries of type {@code S} that reference the given proxy.
     *
     * @param context the event source context, providing access to the Kubernetes client
     * @param proxy the proxy whose referencing secondaries are sought
     * @return a predicate that is {@code true} for secondaries referencing {@code proxy}
     */
    Predicate<S> referencingSecondaries(EventSourceContext<KafkaProxy> context, KafkaProxy proxy);
}
