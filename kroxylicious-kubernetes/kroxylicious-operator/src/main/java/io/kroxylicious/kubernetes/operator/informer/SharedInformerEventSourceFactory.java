/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.informer;

import java.util.Set;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

/**
 * Factory methods for creating SharedInformerEventSource instances.
 * This eliminates code duplication across reconcilers.
 */
public final class SharedInformerEventSourceFactory {

    private SharedInformerEventSourceFactory() {
        // Utility class
    }

    /**
     * Creates a SharedInformerEventSource with the given parameters.
     *
     * @param <P> the primary resource type
     * @param <R> the secondary resource type
     * @param resourceClass the secondary resource class
     * @param eventSourceName the event source name
     * @param sharedInformer the shared informer
     * @param primaryToSecondaryMapper mapper from primary to secondary resources
     * @param secondaryToPrimaryMapper mapper from secondary to primary resources
     * @param allowedNamespaces namespaces to filter events (empty means all namespaces)
     * @return a new SharedInformerEventSource instance
     */
    public static <P extends HasMetadata, R extends HasMetadata> SharedInformerEventSource<P, R> createSharedInformerEventSource(
                                                                                                                                 Class<R> resourceClass,
                                                                                                                                 String eventSourceName,
                                                                                                                                 SharedIndexInformer<R> sharedInformer,
                                                                                                                                 PrimaryToSecondaryMapper<P> primaryToSecondaryMapper,
                                                                                                                                 SecondaryToPrimaryMapper<R> secondaryToPrimaryMapper,
                                                                                                                                 Set<String> allowedNamespaces) {
        return new SharedInformerEventSource<>(
                resourceClass,
                eventSourceName,
                sharedInformer,
                primaryToSecondaryMapper,
                secondaryToPrimaryMapper,
                allowedNamespaces);
    }
}
