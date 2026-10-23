/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.eventsource;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

/**
 * A small DSL for declaring how a primary resource of type {@code P} and a secondary resource of type
 * {@code S} are related, producing the {@link Relationship} consumed by
 * {@link EventSourceBuilder#withRelationship(Relationship)}. Every declarative relationship a reconciler needs
 * to express, regardless of which side holds the pointer, is built via one of these factories:
 * <ul>
 *     <li>{@link #toSecondary(Function)} &mdash; the primary holds a single optional {@link LocalRef} to the
 *     secondary (e.g. {@code VirtualKafkaCluster.spec.targetKafkaServiceRef}).</li>
 *     <li>{@link #toSecondaries(Function)} &mdash; the primary holds zero or more {@link LocalRef}s to
 *     secondaries (e.g. {@code VirtualKafkaCluster.spec.filterRefs}).</li>
 *     <li>{@link #toPrimary(Function)} &mdash; the secondary holds a {@link LocalRef} to the primary directly
 *     (e.g. {@code KafkaProxyIngress.spec.proxyRef}).</li>
 *     <li>{@link #toPrimaryVia(Class, BiPredicate, Relationship)} &mdash; the secondary is referenced by some
 *     intermediate resource which, in turn, references the primary (e.g. {@code KafkaProtocolFilter} is
 *     referenced by a {@code VirtualKafkaCluster.spec.filterRefs} whose {@code proxyRef} points at the
 *     primary).</li>
 * </ul>
 * References are matched by name within the primary's namespace, consistent with the rest of the operator.
 */
public final class Ref {

    private Ref() {
    }

    /**
     * A relationship where the primary holds a single optional reference to the secondary.
     *
     * @param ref extracts the primary's optional reference to the secondary
     * @param <P> the type of the primary resource
     * @param <S> the type of the secondary resource
     * @return a relationship where the primary names its secondary directly
     */
    public static <P extends HasMetadata, S extends HasMetadata> Relationship<P, S> toSecondary(Function<P, Optional<LocalRef<S>>> ref) {
        return new Relationship<>() {
            @Override
            public PrimaryToSecondaryMapper<P> primaryToSecondary(EventSourceContext<P> context, Class<S> secondaryType) {
                return primary -> ref.apply(primary).map(r -> ResourcesUtil.localRefAsResourceId(primary, r)).orElse(Set.of());
            }

            @Override
            public SecondaryToPrimaryMapper<S> secondaryToPrimary(EventSourceContext<P> context) {
                return secondary -> ResourcesUtil.findKnownPrimariesOf(context, secondary, ref);
            }
        };
    }

    /**
     * A relationship where the primary holds zero or more references to secondaries.
     *
     * @param refs extracts the primary's references to its secondaries (may return {@code null} or empty)
     * @param <P> the type of the primary resource
     * @param <S> the type of the secondary resource
     * @return a relationship where the primary names its secondaries directly
     */
    public static <P extends HasMetadata, S extends HasMetadata> Relationship<P, S> toSecondaries(
                                                                                                  Function<P, ? extends Collection<? extends LocalRef<S>>> refs) {
        return new Relationship<>() {
            @Override
            public PrimaryToSecondaryMapper<P> primaryToSecondary(EventSourceContext<P> context, Class<S> secondaryType) {
                return primary -> {
                    Collection<? extends LocalRef<S>> rs = refs.apply(primary);
                    return rs == null || rs.isEmpty() ? Set.of() : ResourcesUtil.localRefsAsResourceIds(primary, rs);
                };
            }

            @Override
            public SecondaryToPrimaryMapper<S> secondaryToPrimary(EventSourceContext<P> context) {
                return secondary -> ResourcesUtil.findKnownPrimariesOfEach(context, secondary, refs);
            }
        };
    }

    /**
     * A direct relationship: the secondary references the primary through a single {@link LocalRef} field.
     *
     * @param ref extracts the secondary's reference to its primary
     * @param <P> the type of the primary resource
     * @param <S> the type of the secondary resource
     * @return a relationship matching secondaries whose reference names the primary
     */
    public static <P extends HasMetadata, S extends HasMetadata> Relationship<P, S> toPrimary(Function<S, ? extends LocalRef<P>> ref) {
        return (ReferencingRelationship<P, S>) (context, primary) -> secondary -> Optional.ofNullable(ref.apply(secondary))
                .map(r -> ResourcesUtil.isReferent(r, primary))
                .orElse(false);
    }

    /**
     * A transitive relationship: the secondary is referenced by an intermediate resource which itself references
     * the primary. The intermediate resources that reference the primary are listed once per primary, so the
     * returned predicate is cheap to evaluate per secondary.
     *
     * @param intermediateType the type of the intermediate resource (e.g. {@code VirtualKafkaCluster})
     * @param intermediateRefersToSecondary tests whether an intermediate references a given secondary
     * @param intermediateToPrimary how the intermediate is related to the primary (built via {@link #toPrimary}
     *     or {@link #toPrimaryVia})
     * @param <P> the type of the primary resource
     * @param <S> the type of the secondary resource
     * @param <I> the type of the intermediate resource
     * @return a relationship matching secondaries reachable from the primary via a matching intermediate
     * @throws IllegalArgumentException if {@code intermediateToPrimary} was not built via {@link #toPrimary} or
     *     {@link #toPrimaryVia}
     */
    public static <P extends HasMetadata, S extends HasMetadata, I extends HasMetadata> Relationship<P, S> toPrimaryVia(
                                                                                                                        Class<I> intermediateType,
                                                                                                                        BiPredicate<I, S> intermediateRefersToSecondary,
                                                                                                                        Relationship<P, I> intermediateToPrimary) {
        if (!(intermediateToPrimary instanceof ReferencingRelationship)) {
            throw new IllegalArgumentException("intermediateToPrimary must be built via Ref.toPrimary or Ref.toPrimaryVia");
        }
        @SuppressWarnings("unchecked")
        ReferencingRelationship<P, I> referencingIntermediateToPrimary = (ReferencingRelationship<P, I>) intermediateToPrimary;

        return (ReferencingRelationship<P, S>) (context, primary) -> {
            List<I> intermediates = ResourcesUtil.resourcesInSameNamespace(context, primary, intermediateType)
                    .filter(referencingIntermediateToPrimary.referencingSecondaries(context, primary))
                    .toList();
            return secondary -> intermediates.stream()
                    .anyMatch(intermediate -> intermediateRefersToSecondary.test(intermediate, secondary));
        };
    }

    /**
     * A predicate testing whether {@code owner} references {@code referent} through a single {@link LocalRef} field.
     * Useful as the {@code intermediateRefersToSecondary} argument to {@link #toPrimaryVia}.
     *
     * @param ref extracts the owner's reference
     * @param <O> the type of the referring resource
     * @param <R> the type of the referent resource
     * @return a predicate matching owners whose reference names the referent
     */
    public static <O, R extends HasMetadata> BiPredicate<O, R> refersTo(Function<O, ? extends LocalRef<?>> ref) {
        return (owner, referent) -> Optional.ofNullable(ref.apply(owner))
                .map(localRef -> ResourcesUtil.isReferent(localRef, referent))
                .orElse(false);
    }

    /**
     * A predicate testing whether {@code owner} references {@code referent} through any of a collection of
     * {@link LocalRef} fields. Useful as the {@code intermediateRefersToSecondary} argument to {@link #toPrimaryVia}.
     *
     * @param refs extracts the owner's references (may return {@code null})
     * @param <O> the type of the referring resource
     * @param <R> the type of the referent resource
     * @return a predicate matching owners any of whose references names the referent
     */
    public static <O, R extends HasMetadata> BiPredicate<O, R> refersToAny(Function<O, Collection<? extends LocalRef<?>>> refs) {
        return (owner, referent) -> Optional.ofNullable(refs.apply(owner)).orElse(List.of())
                .stream()
                .filter(Objects::nonNull)
                .anyMatch(ref -> ResourcesUtil.isReferent(ref, referent));
    }
}
