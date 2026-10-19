/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

/**
 * A small DSL for declaring how a secondary resource is related to a {@link KafkaProxy}, producing the
 * {@link ProxyRelationship} consumed by {@link ReferencingSecondaryMapper}.
 * <ul>
 *     <li>{@link #direct(Function)} — the secondary holds a {@link LocalRef} to the proxy directly
 *     (e.g. {@code KafkaProxyIngress.spec.proxyRef}).</li>
 *     <li>{@link #via(Class, BiPredicate, ProxyRelationship)} — the secondary is referenced by some intermediate
 *     resource which, in turn, references the proxy (e.g. {@code KafkaProtocolFilter} is referenced by a
 *     {@code VirtualKafkaCluster.spec.filterRefs} whose {@code proxyRef} points at the proxy).</li>
 * </ul>
 * References are matched by name within the proxy's namespace, consistent with the rest of the operator.
 */
final class Ref {

    private Ref() {
    }

    /**
     * A direct relationship: the secondary references the proxy through a single {@link LocalRef} field.
     *
     * @param proxyRef extracts the secondary's reference to its proxy
     * @param <S> the type of the secondary resource
     * @return a relationship matching secondaries whose {@code proxyRef} names the proxy
     */
    static <S extends HasMetadata> ProxyRelationship<S> direct(Function<S, ? extends LocalRef<KafkaProxy>> proxyRef) {
        return (context, proxy) -> secondary -> ResourcesUtil.isReferent(proxyRef.apply(secondary), proxy);
    }

    /**
     * A transitive relationship: the secondary is referenced by an intermediate resource which itself references
     * the proxy. The intermediate resources that reference the proxy are listed once per proxy, so the returned
     * predicate is cheap to evaluate per secondary.
     *
     * @param intermediateType the type of the intermediate resource (e.g. {@code VirtualKafkaCluster})
     * @param intermediateRefersToSecondary tests whether an intermediate references a given secondary
     * @param intermediateToProxy how the intermediate is related to the proxy (typically {@link #direct})
     * @param <S> the type of the secondary resource
     * @param <I> the type of the intermediate resource
     * @return a relationship matching secondaries reachable from the proxy via a matching intermediate
     */
    static <S extends HasMetadata, I extends HasMetadata> ProxyRelationship<S> via(
                                                                                   Class<I> intermediateType,
                                                                                   BiPredicate<I, S> intermediateRefersToSecondary,
                                                                                   ProxyRelationship<I> intermediateToProxy) {
        return (context, proxy) -> {
            List<I> intermediates = ResourcesUtil.resourcesInSameNamespace(context, proxy, intermediateType)
                    .filter(intermediateToProxy.referencingSecondaries(context, proxy))
                    .toList();
            return secondary -> intermediates.stream()
                    .anyMatch(intermediate -> intermediateRefersToSecondary.test(intermediate, secondary));
        };
    }

    /**
     * A predicate testing whether {@code owner} references {@code referent} through a single {@link LocalRef} field.
     * Useful as the {@code intermediateRefersToSecondary} argument to {@link #via}.
     *
     * @param ref extracts the owner's reference
     * @param <O> the type of the referring resource
     * @param <R> the type of the referent resource
     * @return a predicate matching owners whose reference names the referent
     */
    static <O, R extends HasMetadata> BiPredicate<O, R> refersTo(Function<O, ? extends LocalRef<?>> ref) {
        return (owner, referent) -> ResourcesUtil.isReferent(ref.apply(owner), referent);
    }

    /**
     * A predicate testing whether {@code owner} references {@code referent} through any of a collection of
     * {@link LocalRef} fields. Useful as the {@code intermediateRefersToSecondary} argument to {@link #via}.
     *
     * @param refs extracts the owner's references (may return {@code null})
     * @param <O> the type of the referring resource
     * @param <R> the type of the referent resource
     * @return a predicate matching owners any of whose references names the referent
     */
    static <O, R extends HasMetadata> BiPredicate<O, R> refersToAny(Function<O, Collection<? extends LocalRef<?>>> refs) {
        return (owner, referent) -> Optional.ofNullable(refs.apply(owner)).orElse(List.of())
                .stream()
                .anyMatch(ref -> ResourcesUtil.isReferent(ref, referent));
    }
}
