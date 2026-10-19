/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

/**
 * Maps a changed {@link KafkaProxy} to the secondary resources of type {@code S} that reference it, so those
 * secondaries are reconciled. The notion of "references" is supplied as a {@link ProxyRelationship}, letting a
 * single mapper serve every secondary type (direct and transitive alike).
 *
 * @param <S> the type of the secondary resource
 */
class ReferencingSecondaryMapper<S extends HasMetadata> implements PrimaryToSecondaryMapper<KafkaProxy> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReferencingSecondaryMapper.class);

    private final EventSourceContext<KafkaProxy> context;
    private final Class<S> secondaryType;
    private final ProxyRelationship<S> relationship;

    ReferencingSecondaryMapper(EventSourceContext<KafkaProxy> context, Class<S> secondaryType, ProxyRelationship<S> relationship) {
        this.context = context;
        this.secondaryType = secondaryType;
        this.relationship = relationship;
    }

    @Override
    public Set<ResourceID> toSecondaryResourceIDs(KafkaProxy proxy) {
        Set<ResourceID> secondaryIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, proxy, secondaryType,
                relationship.referencingSecondaries(context, proxy));
        LOGGER.atDebug()
                .addKeyValue("kind", HasMetadata.getKind(secondaryType))
                .addKeyValue("secondaryIds", secondaryIds)
                .log("Mapped proxy to referencing secondaries");
        return secondaryIds;
    }
}
