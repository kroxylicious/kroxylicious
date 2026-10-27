/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.eventsource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

/**
 *
 */
public interface Dependency<P extends HasMetadata, S extends HasMetadata> {

    /**
     * Returns the mapper that, given a primary, finds the secondaries related to it.
     *
     * @param context the event source context
     * @param secondaryType the class of the secondary resource
     * @return a mapper from primary to related secondary resource IDs
     */
    PrimaryToSecondaryMapper<P> primaryToSecondary(EventSourceContext<P> context, Class<S> secondaryType);

    /**
     * Returns the mapper that, given a secondary, finds the primaries related to it.
     *
     * @param context the event source context
     * @return a mapper from secondary to related primary resource IDs
     */
    SecondaryToPrimaryMapper<S> secondaryToPrimary(EventSourceContext<P> context);

    String eventSourceName();

    EventSourceBuilder<P, S> eventSourceBuilder(EventSourceContext<P> context);

}
