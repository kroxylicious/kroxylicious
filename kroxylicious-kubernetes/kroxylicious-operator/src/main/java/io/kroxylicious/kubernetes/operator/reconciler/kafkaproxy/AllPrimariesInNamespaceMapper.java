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
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.findAllKnownPrimariesInNamespace;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * Maps any secondary resource event to all {@link KafkaProxy} primaries in the same namespace, ignoring events
 * from secondaries whose status has not yet caught up with their spec.
 * <p>
 * This is identical for every secondary type the {@code KafkaProxy} reconciler watches: none of them point at a
 * specific proxy in a way that lets us narrow the set (and reconciling an unrelated proxy is a harmless no-op),
 * so a single instance is shared across all of them. Because the mapper only ever <em>reads</em> the secondary,
 * it is safe to view the single {@code SecondaryToPrimaryMapper<HasMetadata>} instance as a mapper for any
 * concrete secondary type via {@link #forSecondary()}.
 */
class AllPrimariesInNamespaceMapper implements SecondaryToPrimaryMapper<HasMetadata> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AllPrimariesInNamespaceMapper.class);

    private final EventSourceContext<KafkaProxy> context;

    AllPrimariesInNamespaceMapper(EventSourceContext<KafkaProxy> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(HasMetadata secondary) {
        // we do not want to trigger reconciliation of any proxy if the secondary has not been reconciled
        if (!ResourcesUtil.isStatusFresh(secondary)) {
            LOGGER.atDebug()
                    .addKeyValue("kind", HasMetadata.getKind(secondary.getClass()))
                    .addKeyValue("name", name(secondary))
                    .addKeyValue("namespace", namespace(secondary))
                    .log("Ignoring event from secondary with stale status");
            return Set.of();
        }
        // the secondary does not identify a single proxy, so reconcile every proxy in its namespace; this also
        // ensures a previously-referenced proxy is updated when a reference is removed (see e.g. #4017)
        Set<ResourceID> proxyIds = findAllKnownPrimariesInNamespace(context, secondary);
        LOGGER.atDebug()
                .addKeyValue("kind", HasMetadata.getKind(secondary.getClass()))
                .addKeyValue("proxyIds", proxyIds)
                .log("Mapped secondary to all proxies in namespace");
        return proxyIds;
    }

    /**
     * Returns this shared instance typed as a mapper for the concrete secondary type {@code S}. Safe because the
     * mapper uses its input only in a read (contravariant) position; the cast merely narrows the declared type to
     * satisfy {@code InformerEventSourceConfiguration.Builder}, which is invariant in the secondary type.
     *
     * @param <S> the concrete secondary type
     * @return this instance, viewed as a {@code SecondaryToPrimaryMapper<S>}
     */
    @SuppressWarnings("unchecked")
    <S extends HasMetadata> SecondaryToPrimaryMapper<S> forSecondary() {
        return (SecondaryToPrimaryMapper<S>) this;
    }
}
