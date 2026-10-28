/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.eventsource;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

/**
 * One edge of a {@link ReferenceDependency}: a relationship between a {@code left} and a {@code right} resource type,
 * authored in the only direction a {@link LocalRef} field actually allows &mdash; from whichever side owns
 * the field, naming whichever side it points at. {@link #leftIsOwner()} records which side that was; both
 * {@link #rightsOf} and {@link #leftsOf} are always available regardless, by scanning for the non-owning side.
 * {@link #label()} is the caller-supplied name for this hop, used to derive an event source name (see
 * {@link ReferenceDependency#eventSourceName()}).
 */
final class Hop {

    private final Class<? extends HasMetadata> leftType;
    private final Class<? extends HasMetadata> rightType;
    private final boolean leftIsOwner;
    private final String label;
    private final Function<HasMetadata, Stream<LocalRef<?>>> ownerToRefs;

    private Hop(Class<? extends HasMetadata> leftType, Class<? extends HasMetadata> rightType, boolean leftIsOwner, String label,
                Function<HasMetadata, Stream<LocalRef<?>>> ownerToRefs) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.leftIsOwner = leftIsOwner;
        this.label = label;
        this.ownerToRefs = ownerToRefs;
    }

    static <L extends HasMetadata, R extends HasMetadata> Hop leftOwnsOne(Class<L> leftType, Class<R> rightType,
                                                                          Function<L, Optional<? extends LocalRef<R>>> ref, String label) {
        return new Hop(leftType, rightType, true, label,
                owner -> ref.apply(leftType.cast(owner)).stream().map(r -> (LocalRef<?>) r));
    }

    static <L extends HasMetadata, R extends HasMetadata> Hop leftOwnsMany(Class<L> leftType, Class<R> rightType,
                                                                           Function<L, ? extends Collection<? extends LocalRef<R>>> refs, String label) {
        return new Hop(leftType, rightType, true, label, owner -> {
            Collection<? extends LocalRef<R>> rs = refs.apply(leftType.cast(owner));
            return rs == null ? Stream.empty() : rs.stream().filter(Objects::nonNull).map(r -> (LocalRef<?>) r);
        });
    }

    /** Mirrors {@link #leftOwnsOne}: the hop's owning side is {@code right}, pointing back at {@code left}. */
    static <L extends HasMetadata, R extends HasMetadata> Hop rightOwnsOne(Class<L> leftType, Class<R> rightType,
                                                                           Function<R, Optional<? extends LocalRef<L>>> ref, String label) {
        return new Hop(leftType, rightType, false, label,
                owner -> ref.apply(rightType.cast(owner)).stream().map(r -> (LocalRef<?>) r));
    }

    Class<? extends HasMetadata> leftType() {
        return leftType;
    }

    boolean leftIsOwner() {
        return leftIsOwner;
    }

    String label() {
        return label;
    }

    /**
     * True if {@code owner} (which must be the actual owning side of this hop) has a reference naming
     * {@code referent}.
     */
    boolean ownerRefersTo(HasMetadata owner, HasMetadata referent) {
        return ownerToRefs.apply(owner).anyMatch(ref -> ResourcesUtil.isReferent(ref, referent));
    }

    /**
     * The (zero or more) right-type resources, in {@code left}'s namespace, that this hop relates to {@code left}.
     * Short-circuits without scanning if {@code left} is the owning side and holds no reference at all.
     */
    Stream<HasMetadata> rightsOf(EventSourceContext<?> context, HasMetadata left) {
        if (leftIsOwner && ownerToRefs.apply(left).findAny().isEmpty()) {
            return Stream.empty();
        }
        return scan(context, left, rightType, right -> matches(left, right));
    }

    /**
     * The resource IDs this hop relates to {@code left}. When {@code left} is the owning side, these are
     * computed directly from its reference(s) &mdash; no scan, and no {@link EventSourceContext#getClient()}
     * call, needed. Otherwise falls back to {@link #rightsOf}, since finding the owning side requires a scan.
     */
    Set<ResourceID> rightIdsOf(EventSourceContext<?> context, HasMetadata left) {
        if (leftIsOwner) {
            return ownerToRefs.apply(left)
                    .map(ref -> new ResourceID(ref.getName(), ResourcesUtil.namespace(left)))
                    .collect(Collectors.toSet());
        }
        return rightsOf(context, left).map(ResourceID::fromResource).collect(Collectors.toSet());
    }

    /**
     * The (zero or more) left-type resources, in {@code right}'s namespace, that this hop relates to {@code right}.
     * Short-circuits without scanning if {@code right} is the owning side and holds no reference at all.
     */
    Stream<HasMetadata> leftsOf(EventSourceContext<?> context, HasMetadata right) {
        if (!leftIsOwner && ownerToRefs.apply(right).findAny().isEmpty()) {
            return Stream.empty();
        }
        return scan(context, right, leftType, left -> matches(left, right));
    }

    private boolean matches(HasMetadata left, HasMetadata right) {
        return leftIsOwner ? ownerRefersTo(left, right) : ownerRefersTo(right, left);
    }

    private static Stream<HasMetadata> scan(EventSourceContext<?> context, HasMetadata inNamespaceOf, Class<? extends HasMetadata> type,
                                            Predicate<HasMetadata> predicate) {
        return ResourcesUtil.resourcesInSameNamespace(context, inNamespaceOf, type)
                .map(HasMetadata.class::cast)
                .filter(predicate);
    }
}
