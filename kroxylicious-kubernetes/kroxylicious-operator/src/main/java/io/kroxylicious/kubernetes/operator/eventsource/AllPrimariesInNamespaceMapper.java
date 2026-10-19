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
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.findAllKnownPrimariesInNamespace;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * Maps any secondary resource event to all primaries of type {@code P} in the same namespace, ignoring events
 * from secondaries whose status has not yet caught up with their spec.
 * <p>
 * This is the safe fallback for the "secondary holds the reference" relationship family
 * ({@link Ref#toPrimary}/{@link Ref#toPrimaryVia}): the secondary does not identify a single primary in a way
 * that survives the reference being changed or removed (an update event only exposes the secondary's current
 * state), so every primary in its namespace is reconciled instead; reconciling an unrelated primary is a
 * harmless no-op. Because the mapper only ever <em>reads</em> the secondary, it is safe to view a single
 * instance as a mapper for any concrete secondary type via {@link #forSecondary()}.
 *
 * @param <P> the type of the primary resource
 */
class AllPrimariesInNamespaceMapper<P extends HasMetadata> implements SecondaryToPrimaryMapper<HasMetadata> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AllPrimariesInNamespaceMapper.class);

    private final EventSourceContext<P> context;

    AllPrimariesInNamespaceMapper(EventSourceContext<P> context) {
        this.context = context;
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(HasMetadata secondary) {
        // we do not want to trigger reconciliation of any primary if the secondary has not been reconciled
        if (!ResourcesUtil.isStatusFresh(secondary)) {
            LOGGER.atDebug()
                    .addKeyValue("kind", HasMetadata.getKind(secondary.getClass()))
                    .addKeyValue("name", name(secondary))
                    .addKeyValue("namespace", namespace(secondary))
                    .log("Ignoring event from secondary with stale status");
            return Set.of();
        }
        // the secondary does not identify a single primary, so reconcile every primary in its namespace; this
        // also ensures a previously-referenced primary is updated when a reference is removed (see e.g. #4017)
        Set<ResourceID> primaryIds = findAllKnownPrimariesInNamespace(context, secondary);
        LOGGER.atDebug()
                .addKeyValue("kind", HasMetadata.getKind(secondary.getClass()))
                .addKeyValue("primaryIds", primaryIds)
                .log("Mapped secondary to all primaries in namespace");
        return primaryIds;
    }

    /**
     * Returns this shared instance typed as a mapper for the concrete secondary type {@code S}. Safe because the
     * mapper uses its input only in a read (contravariant) position; the cast merely narrows the declared type to
     * satisfy call sites which are invariant in the secondary type.
     *
     * @param <S> the concrete secondary type
     * @return this instance, viewed as a {@code SecondaryToPrimaryMapper<S>}
     */
    @SuppressWarnings("unchecked")
    <S extends HasMetadata> SecondaryToPrimaryMapper<S> forSecondary() {
        return (SecondaryToPrimaryMapper<S>) this;
    }
}
