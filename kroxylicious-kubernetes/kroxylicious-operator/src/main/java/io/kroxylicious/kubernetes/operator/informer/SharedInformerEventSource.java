/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.informer;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.javaoperatorsdk.operator.processing.event.Event;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.AbstractEventSource;
import io.javaoperatorsdk.operator.processing.event.source.Cache;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

/**
 * An EventSource that wraps a shared Fabric8 SharedIndexInformer.
 * <p>
 * This allows multiple reconcilers to share the same underlying informer cache
 * while each having their own event handling and mapping logic.
 *
 * @param <R> the secondary resource type (e.g., Secret)
 * @param <P> the primary resource type (e.g., KafkaService)
 */
public class SharedInformerEventSource<R extends HasMetadata, P extends HasMetadata>
        extends AbstractEventSource<R, P>
        implements Cache<R>, ResourceEventHandler<R> {

    private final SharedIndexInformer<R> sharedInformer;
    private final SecondaryToPrimaryMapper<R> secondaryToPrimaryMapper;
    private final PrimaryToSecondaryMapper<P> primaryToSecondaryMapper;
    private final Set<String> allowedNamespaces;

    /**
     * Creates a SharedInformerEventSource.
     *
     * @param resourceClass the secondary resource class
     * @param name the event source name
     * @param sharedInformer the shared Fabric8 informer
     * @param primaryToSecondaryMapper mapper to determine which secondary resources are related to a primary resource
     * @param secondaryToPrimaryMapper mapper to determine which primary resources are affected by secondary resource changes
     * @param allowedNamespaces namespaces to filter events (empty means all namespaces)
     */
    public SharedInformerEventSource(
                                     Class<R> resourceClass,
                                     String name,
                                     SharedIndexInformer<R> sharedInformer,
                                     PrimaryToSecondaryMapper<P> primaryToSecondaryMapper,
                                     SecondaryToPrimaryMapper<R> secondaryToPrimaryMapper,
                                     Set<String> allowedNamespaces) {
        super(resourceClass, name);
        this.sharedInformer = Objects.requireNonNull(sharedInformer);
        this.secondaryToPrimaryMapper = Objects.requireNonNull(secondaryToPrimaryMapper);
        this.primaryToSecondaryMapper = Objects.requireNonNull(primaryToSecondaryMapper);
        this.allowedNamespaces = Objects.requireNonNull(allowedNamespaces);
    }

    /**
     * Checks if a resource is in an allowed namespace.
     *
     * @param resource the resource to check
     * @return true if allowed (empty set means all namespaces allowed)
     */
    private boolean isAllowedNamespace(R resource) {
        if (allowedNamespaces.isEmpty()) {
            return true; // All namespaces allowed
        }
        String namespace = resource.getMetadata().getNamespace();
        return allowedNamespaces.contains(namespace);
    }

    @Override
    public void start() {
        // Register this as an event handler on the shared informer
        sharedInformer.addEventHandler(this);
    }

    @Override
    public void stop() {
        // Remove this event handler from the shared informer
        sharedInformer.removeEventHandler(this);
        // Note: We don't stop the shared informer as other event sources may be using it
        // The SharedInformerManager is responsible for stopping shared informers
    }

    // ResourceEventHandler implementation - handles events from the shared informer

    @Override
    public void onAdd(R resource) {
        // Filter by namespace
        if (!isAllowedNamespace(resource)) {
            return;
        }

        // Map the secondary resource to affected primary resources
        Set<ResourceID> primaryResourceIDs = secondaryToPrimaryMapper.toPrimaryResourceIDs(resource);

        // Trigger reconciliation for each affected primary resource
        primaryResourceIDs.forEach(primaryID -> getEventHandler().handleEvent(new Event(primaryID)));
    }

    @Override
    public void onUpdate(R oldResource, R newResource) {
        // Filter by namespace - check new resource (old might have been in different namespace)
        if (!isAllowedNamespace(newResource)) {
            return;
        }

        // For updates, we need to handle both the old and new resource
        Set<ResourceID> oldPrimaryIDs = secondaryToPrimaryMapper.toPrimaryResourceIDs(oldResource);
        Set<ResourceID> newPrimaryIDs = secondaryToPrimaryMapper.toPrimaryResourceIDs(newResource);

        // Trigger reconciliation for all affected primary resources
        Stream.concat(oldPrimaryIDs.stream(), newPrimaryIDs.stream())
                .distinct()
                .forEach(primaryID -> getEventHandler().handleEvent(new Event(primaryID)));
    }

    @Override
    public void onDelete(R resource, boolean deletedFinalStateUnknown) {
        // Filter by namespace
        if (!isAllowedNamespace(resource)) {
            return;
        }

        Set<ResourceID> primaryResourceIDs = secondaryToPrimaryMapper.toPrimaryResourceIDs(resource);

        primaryResourceIDs.forEach(primaryID -> getEventHandler().handleEvent(new Event(primaryID)));
    }

    @Override
    public Optional<R> get(ResourceID resourceID) {
        String key = resourceID.getNamespace().orElse("") + "/" + resourceID.getName();
        return Optional.ofNullable(sharedInformer.getStore().getByKey(key));
    }

    @Override
    public Stream<ResourceID> keys() {
        return sharedInformer.getStore().list().stream()
                .map(ResourceID::fromResource);
    }

    @Override
    public Stream<R> list(Predicate<R> predicate) {
        return sharedInformer.getStore().list().stream()
                .filter(predicate);
    }

    @Override
    public Set<R> getSecondaryResources(P primary) {
        // Use the primary-to-secondary mapper to get ResourceIDs of related secondary resources
        Set<ResourceID> secondaryResourceIDs = primaryToSecondaryMapper.toSecondaryResourceIDs(primary);

        // Look up each ResourceID from the shared informer's cache
        return secondaryResourceIDs.stream()
                .map(this::get)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
    }
}
