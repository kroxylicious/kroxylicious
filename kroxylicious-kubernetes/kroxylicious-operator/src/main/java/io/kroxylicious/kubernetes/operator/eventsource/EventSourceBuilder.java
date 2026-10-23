/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.eventsource;

import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.informer.SharedInformerEventSource;
import io.kroxylicious.kubernetes.operator.informer.SharedInformerManager;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Fluent builder for registering a single JOSDK event source: the secondary {@link Class} literal, the event
 * source name, shared-vs-dedicated informer choice, and the reference-traversal relationship (declared via
 * {@link Ref}, or a bespoke mapper pair for relationships that aren't expressible as a same-namespace
 * {@link io.kroxylicious.kubernetes.api.common.LocalRef} lookup).
 *
 * @param <P> the type of the primary resource
 * @param <S> the type of the secondary resource
 */
public final class EventSourceBuilder<P extends HasMetadata, S extends HasMetadata> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventSourceBuilder.class);

    private final EventSourceContext<P> context;
    private final Class<S> secondaryType;
    private @Nullable String name;
    private @Nullable SharedInformerManager sharedInformerManager;
    private boolean skipFreshStatus;
    private @Nullable PrimaryToSecondaryMapper<P> primaryToSecondaryMapper;
    private @Nullable SecondaryToPrimaryMapper<S> secondaryToPrimaryMapper;

    private EventSourceBuilder(EventSourceContext<P> context, Class<S> secondaryType) {
        this.context = Objects.requireNonNull(context);
        this.secondaryType = Objects.requireNonNull(secondaryType);
    }

    /**
     * Starts building an event source for the given secondary type.
     *
     * @param context the event source context
     * @param secondaryType the class of the secondary resource
     * @param <P> the type of the primary resource
     * @param <S> the type of the secondary resource
     * @return a new builder
     */
    public static <P extends HasMetadata, S extends HasMetadata> EventSourceBuilder<P, S> forSecondary(EventSourceContext<P> context, Class<S> secondaryType) {
        return new EventSourceBuilder<>(context, secondaryType);
    }

    /**
     * Sets the event source name. Mandatory: several reconcilers register more than one event source for the
     * same secondary class, and {@code context.getSecondaryResource(Class, name)} call sites depend on exact
     * name strings, so there is no safe default to fall back to.
     *
     * @param name the event source name
     * @return this builder
     */
    public EventSourceBuilder<P, S> withName(String name) {
        this.name = Objects.requireNonNull(name);
        return this;
    }

    /**
     * Uses a cache shared with other reconcilers watching the same secondary type, instead of a dedicated
     * informer.
     *
     * @param sharedInformerManager the shared informer manager
     * @return this builder
     */
    public EventSourceBuilder<P, S> withSharedInformer(SharedInformerManager sharedInformerManager) {
        this.sharedInformerManager = Objects.requireNonNull(sharedInformerManager);
        return this;
    }

    /**
     * Declares the relationship between primary and secondary, built via {@link Ref}. The only declarative path;
     * mutually exclusive with {@link #withMappers(PrimaryToSecondaryMapper, SecondaryToPrimaryMapper)}.
     *
     * @param relationship the relationship, as built by one of the {@link Ref} factories
     * @return this builder
     */
    public EventSourceBuilder<P, S> withRelationship(Relationship<P, S> relationship) {
        requireMappersNotYetSet();
        this.primaryToSecondaryMapper = relationship.primaryToSecondary(context, secondaryType);
        this.secondaryToPrimaryMapper = relationship.secondaryToPrimary(context);
        return this;
    }

    /**
     * Escape hatch for relationships not expressible via {@link Ref}: cross-namespace indexed lookups,
     * owner-reference/derived-name correlation, interpolation-driven discovery, and similar bespoke logic.
     * Mutually exclusive with {@link #withRelationship(Relationship)}.
     *
     * @param primaryToSecondaryMapper the primary-to-secondary mapper
     * @param secondaryToPrimaryMapper the secondary-to-primary mapper
     * @return this builder
     */
    public EventSourceBuilder<P, S> withMappers(PrimaryToSecondaryMapper<P> primaryToSecondaryMapper, SecondaryToPrimaryMapper<S> secondaryToPrimaryMapper) {
        requireMappersNotYetSet();
        this.primaryToSecondaryMapper = Objects.requireNonNull(primaryToSecondaryMapper);
        this.secondaryToPrimaryMapper = Objects.requireNonNull(secondaryToPrimaryMapper);
        return this;
    }

    /**
     * Opts out of the freshness guard that {@link #build()} otherwise applies automatically to secondaries of a
     * status-bearing Kroxylicious custom resource type (see {@link ResourcesUtil#hasObservedGeneration(Class)}).
     * Needed where a primary must be reconciled on every change to such a secondary regardless of whether the
     * secondary's own controller has caught up (e.g. a reconciler watching {@code KafkaProxy} as a secondary).
     * A no-op for core Kubernetes types, which are never guarded.
     *
     * @return this builder
     */
    public EventSourceBuilder<P, S> withoutFreshStatus() {
        this.skipFreshStatus = true;
        return this;
    }

    /**
     * Builds the event source.
     *
     * @return a dedicated {@link InformerEventSource}, or a {@link SharedInformerEventSource} if
     *     {@link #withSharedInformer(SharedInformerManager)} was called
     * @throws IllegalStateException if {@link #withName(String)} was not called, if neither
     *     {@link #withRelationship(Relationship)} nor {@link #withMappers(PrimaryToSecondaryMapper, SecondaryToPrimaryMapper)}
     *     was called, or if either was called more than once
     */
    public EventSource<S, P> build() {
        if (name == null) {
            throw new IllegalStateException("withName(...) must be called");
        }
        if (primaryToSecondaryMapper == null || secondaryToPrimaryMapper == null) {
            throw new IllegalStateException("exactly one of withRelationship(...) or withMappers(...) must be called");
        }
        // Guard status-bearing CRD secondaries unless the caller opted out: don't reconcile a primary off a
        // secondary whose own controller hasn't caught up. Core Kubernetes types have no such status.
        boolean applyFreshnessGuard = ResourcesUtil.hasObservedGeneration(secondaryType) && !skipFreshStatus;
        SecondaryToPrimaryMapper<S> s2p = applyFreshnessGuard ? withFreshnessGuard(secondaryToPrimaryMapper) : secondaryToPrimaryMapper;

        if (sharedInformerManager != null) {
            var informer = sharedInformerManager.getOrCreateInformer(secondaryType);
            return new SharedInformerEventSource<>(secondaryType, name, informer, primaryToSecondaryMapper, s2p, sharedInformerManager.effectiveNamespaces());
        }
        var configuration = InformerEventSourceConfiguration.from(secondaryType, context.getPrimaryResourceClass())
                .withName(name)
                .withPrimaryToSecondaryMapper(primaryToSecondaryMapper)
                .withSecondaryToPrimaryMapper(s2p)
                .build();
        return new InformerEventSource<>(configuration);
    }

    private SecondaryToPrimaryMapper<S> withFreshnessGuard(SecondaryToPrimaryMapper<S> delegate) {
        return secondary -> {
            if (ResourcesUtil.isStatusFresh(secondary)) {
                return delegate.toPrimaryResourceIDs(secondary);
            }
            LOGGER.atDebug()
                    .addKeyValue("kind", HasMetadata.getKind(secondaryType))
                    .addKeyValue("name", ResourcesUtil.name(secondary))
                    .addKeyValue("namespace", ResourcesUtil.namespace(secondary))
                    .log("Ignoring event from secondary with stale status");
            return Set.of();
        };
    }

    private void requireMappersNotYetSet() {
        if (primaryToSecondaryMapper != null || secondaryToPrimaryMapper != null) {
            throw new IllegalStateException("only one of withRelationship(...) or withMappers(...) may be called");
        }
    }
}
