/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.eventsource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A dependency of some primary resources of on some secondary (or tertiary, or more) resource via
 * a reference defined in one of those resources.
 * <p>
 * The "direction of dependence" is always primary depends on secondary, but the either primary or secondary can "own" the reference.
 * "Forward references" are those where the primary's state depends on a secondary and where a primary resource refers (e.g. by name) to a secondary resource.
 * "Reverse references" are those where the primary's state depends on a secondary and where the secondary resource refers (e.g. by name) to a primary resource.
 * Forward references are constructed using {@link #dependsOn(Class, String, Function)} or {@link #dependsOnEach(Class, String, Function)}.
 * Reverse references are constructed using {@link #dependsOnReferrer(Class, String, Function)}.
 * <p>
 * Reference dependencies can be chained to construct indirect dependencies, simply by invoking the {@code dependsOn*()} methods to create more
 * indirect dependencies.
 *
 * @see StaticDependency for another kind of dependency.
 */
public final class ReferenceDependency<P extends HasMetadata, S extends HasMetadata> implements Dependency<P, S> {

    private final Class<P> leftType;
    private final Class<S> rightType;
    private final List<Hop> hops;
    private final boolean pureForward;
    private @Nullable String name = null;

    private ReferenceDependency(Class<P> leftType, Class<S> rightType, List<Hop> hops) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.hops = List.copyOf(hops);
        this.pureForward = hops.stream().allMatch(Hop::leftIsOwner);
    }

    public static <P extends HasMetadata> ReferenceDependency<P, P> each(Class<P> type) {
        return new ReferenceDependency<>(type, type, List.of());
    }

    /**
     * Extend the chain of dependency through a forward reference to a resource of type {@code C}
     * @param type The class of the depended-upon resource
     * @param label The name for this dependency
     * @param ref The function which extracts the reference the current tip of the dependency chain to the next depended-upon resource
     * @return The new dependency
     * @param <C> The type of the depended-upon resource
     */
    public <C extends HasMetadata> ReferenceDependency<P, C> dependsOn(Class<C> type, String label, Function<S, Optional<? extends LocalRef<C>>> ref) {
        return extend(type, Hop.leftOwnsOne(rightType, type, ref, label));
    }

    /**
     * Extend the chain of dependency through a forward reference to a collection resources of type {@code C}
     * @param type The class of the depended-upon resource
     * @param label The name for this dependency
     * @param refs The function which extracts the reference the current tip of the dependency chain to the next depended-upon resources
     * @return The new dependency
     * @param <C> The type of the depended-upon resources
     */
    public <C extends HasMetadata> ReferenceDependency<P, C> dependsOnEach(Class<C> type, String label, Function<S, ? extends Collection<? extends LocalRef<C>>> refs) {
        return extend(type, Hop.leftOwnsMany(rightType, type, refs, label));
    }

    /**
     * Extend the chain of dependency through a reverse reference to a resource of type {@code C}.
     * The depended-upon resource holds the reference to resources at the current tip of the dependency chain.
     * @param type The class of the depended-upon resource
     * @param label The name for this dependency
     * @param ref The function which extracts the reference from the depended-upon resource to the current tip of the dependency chain.
     * @return The new dependency
     * @param <C> The type of the depended-upon resource
     */
    public <C extends HasMetadata> ReferenceDependency<P, C> dependsOnReferrer(Class<C> type, String label, Function<C, Optional<? extends LocalRef<S>>> ref) {
        return extend(type, Hop.rightOwnsOne(rightType, type, ref, label));
    }

    private <C extends HasMetadata> ReferenceDependency<P, C> extend(Class<C> type, Hop hop) {
        List<Hop> combined = new ArrayList<>(hops);
        combined.add(hop);
        return new ReferenceDependency<>(leftType, type, combined);
    }

    /**
     * Gets the default name of the event source returned from {@link #eventSourceBuilder(EventSourceContext)}.
     * This is constructed using the labels for each of the dependencies in the chain,
     * which results in consistent event source names and reduces opportunities for naming collisions.
     * @return Gets the default name of the event source returned from {@link #eventSourceBuilder(EventSourceContext)}.
     */
    public String eventSourceName() {
        String result = this.name;
        if (result == null) {
            // There's a benign data race here, which avoids needing synchronized at the expense of possible
            // redundant computation.
            StringBuilder builder = new StringBuilder(this.leftType.getSimpleName());
            for (Hop hop : this.hops) {
                builder.append(hop.leftIsOwner() ? "->" : "<-").append(hop.label());
            }
            this.name = result = builder.toString();
        }
        return result;
    }

    /**
     * Returns an EventSourceBuilder based on this dependency.
     * @return an EventSourceBuilder based on this dependency.
     */
    public EventSourceBuilder<P, S> eventSourceBuilder(EventSourceContext<P> context) {
        return EventSourceBuilder.forSecondary(context, rightType)
                .withName(eventSourceName())
                .withRelationship(this);
    }

    public PrimaryToSecondaryMapper<P> primaryToSecondary(EventSourceContext<P> context, Class<S> secondaryType) {
        return primary -> {
            if (hops.isEmpty()) {
                return Set.of(ResourceID.fromResource(primary));
            }
            Stream<HasMetadata> current = Stream.of(primary);
            for (int i = 0; i < hops.size() - 1; i++) {
                Hop hop = hops.get(i);
                current = current.flatMap(instance -> hop.rightsOf(context, instance));
            }
            Hop last = hops.get(hops.size() - 1);
            if (pureForward) {
                // A pure forward chain trusts its own final reference, exactly as Ref#toSecondary and
                // Ref#toSecondaryVia did: no scan (and no EventSourceContext#getClient() call) needed,
                // and the referenced secondary need not already exist.
                return current.flatMap(instance -> last.rightIdsOf(context, instance).stream()).collect(Collectors.toSet());
            }
            // Otherwise (some hop was traversed backward) the final secondary is found by scanning
            // its own type and filtering, exactly as Ref#toPrimaryVia did: only secondaries that
            // actually exist are ever returned.
            return current.flatMap(instance -> last.rightsOf(context, instance)).map(ResourceID::fromResource).collect(Collectors.toSet());
        };
    }

    public SecondaryToPrimaryMapper<S> secondaryToPrimary(EventSourceContext<P> context) {
        if (!pureForward) {
            return new AllPrimariesInNamespaceMapper<P>(context).forSecondary();
        }
        return secondary -> {
            Stream<HasMetadata> current = Stream.of(secondary);
            for (int i = hops.size() - 1; i >= 1; i--) {
                Hop hop = hops.get(i);
                current = current.flatMap(instance -> hop.leftsOf(context, instance));
            }
            if (hops.isEmpty()) {
                return current.map(ResourceID::fromResource).collect(Collectors.toSet());
            }
            // The final hop reaches the primary itself: resolve it from the controller's primary cache
            // rather than a live list, for the same reasons as ResourcesUtil.findKnownPrimariesOf.
            Hop first = hops.get(0);
            return current
                    .flatMap(adjacent -> context.getPrimaryCache().list(ResourcesUtil.namespace(adjacent), p -> first.ownerRefersTo(p, adjacent)))
                    .map(ResourceID::fromResource)
                    .collect(Collectors.toSet());
        };
    }
}
