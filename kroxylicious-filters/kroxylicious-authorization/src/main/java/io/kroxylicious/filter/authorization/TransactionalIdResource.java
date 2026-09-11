/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Set;

import io.kroxylicious.authorizer.service.ResourceType;

/**
 * The operations which may be performed on a Kafka transactional id.
 */
public enum TransactionalIdResource implements ResourceType<TransactionalIdResource> {
    /** Produce transactionally using the transactional id. */
    WRITE(4),
    /** Describe transactions with the transactional id. */
    DESCRIBE(8),
    /** Use two-phase commit with the transactional id. */
    TWO_PHASE_COMMIT((byte) 15);

    private static final Set<TransactionalIdResource> DESCRIBE_SET = Set.of(DESCRIBE);

    /** The code used by Kafka to identify the equivalent {@code AclOperation}. */
    public final int kafkaOrdinal;

    TransactionalIdResource(int kafkaOrdinal) {
        this.kafkaOrdinal = kafkaOrdinal;
    }

    @Override
    public Set<TransactionalIdResource> implies() {
        if (this == WRITE) {
            return DESCRIBE_SET;
        }
        else {
            return Set.of();
        }
    }
}
