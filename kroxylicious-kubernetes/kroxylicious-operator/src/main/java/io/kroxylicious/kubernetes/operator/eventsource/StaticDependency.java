/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.eventsource;

import java.util.Set;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.operator.ResourcesUtil;

/**
 * A dependency of some primary resources of on some statically known secondary resource
 * @param <P> The type of the primary
 * @param <S> The type of the secondary
 */
public class StaticDependency<P extends HasMetadata, S extends HasMetadata> implements Dependency<P, S> {
    private final Class<S> secondaryType;
    private final String name;
    private final Class<P> primaryClass;

    StaticDependency(Class<P> primaryClass, Class<S> secondaryType, String name) {
        this.primaryClass = primaryClass;
        this.secondaryType = secondaryType;
        this.name = name;
    }

    public static <P extends HasMetadata> StaticDependency<P, P> each(Class<P> primaryClass) {
        return new StaticDependency<>(primaryClass, primaryClass, "");
    }

    /**
     * All primaries depend on a given cluster-scoped secondary resource
     * @param secondaryType The class of the cluster-scoped secondary resource
     * @param resourceName The name of the cluster-scoped secondary resource
     * @return The dependency
     * @param <R> The type of the cluster-scoped secondary resource
     */
    public <R extends HasMetadata> StaticDependency<P, R> dependsOnClusterResource(Class<R> secondaryType, String resourceName) {
        return new StaticDependency<>(this.primaryClass, secondaryType, resourceName);
    }

    @Override
    public PrimaryToSecondaryMapper<P> primaryToSecondary(EventSourceContext<P> context, Class<S> secondaryType) {
        return primary -> Set.of(new ResourceID(name, null));
    }

    @Override
    public SecondaryToPrimaryMapper<S> secondaryToPrimary(EventSourceContext<P> context) {
        return secondary -> ResourcesUtil.findAllKnownPrimaries(context);
    }

    @Override
    public String eventSourceName() {
        return secondaryType.getSimpleName() + "(" + name + ")";
    }

    public EventSourceBuilder<P, S> eventSourceBuilder(EventSourceContext<P> context) {
        return EventSourceBuilder.forSecondary(context, secondaryType)
                .withName(eventSourceName())
                .withRelationship(this);
    }
}
