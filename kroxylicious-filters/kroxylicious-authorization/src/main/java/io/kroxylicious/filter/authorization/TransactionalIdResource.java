/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Set;

import io.kroxylicious.authorizer.service.ResourceType;

public enum TransactionalIdResource implements ResourceType<TransactionalIdResource> {
    WRITE(4),
    DESCRIBE(8),
    TWO_PHASE_COMMIT((byte) 15);

    private static final Set<TransactionalIdResource> DESCRIBE_SET = Set.of(DESCRIBE);

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
