/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.eventsource;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.operator.ResourcesUtil;

/**
 * Maps a changed primary to the secondary resources of type {@code S} that reference it, so those secondaries
 * are reconciled. The notion of "references" is supplied as a {@link ReferencingRelationship}, letting a single
 * mapper serve every secondary type related to a primary this way (direct and transitive alike).
 *
 * @param <P> the type of the primary resource
 * @param <S> the type of the secondary resource
 */
class ReferencingSecondaryMapper<P extends HasMetadata, S extends HasMetadata> implements PrimaryToSecondaryMapper<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReferencingSecondaryMapper.class);

    private final EventSourceContext<P> context;
    private final Class<S> secondaryType;
    private final ReferencingRelationship<P, S> relationship;

    ReferencingSecondaryMapper(EventSourceContext<P> context, Class<S> secondaryType, ReferencingRelationship<P, S> relationship) {
        this.context = context;
        this.secondaryType = secondaryType;
        this.relationship = relationship;
    }

    @Override
    public Set<ResourceID> toSecondaryResourceIDs(P primary) {
        Set<ResourceID> secondaryIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, primary, secondaryType,
                relationship.referencingSecondaries(context, primary));
        LOGGER.atDebug()
                .addKeyValue("kind", HasMetadata.getKind(secondaryType))
                .addKeyValue("secondaryIds", secondaryIds)
                .log("Mapped primary to referencing secondaries");
        return secondaryIds;
    }
}
