/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import io.kroxylicious.authorizer.service.ResourceType;

public enum TransactionalIdResource implements ResourceType<TransactionalIdResource> {
    WRITE(4),
    DESCRIBE(8),
    TWO_PHASE_COMMIT((byte) 15);

    public final int kafkaOrdinal;

    TransactionalIdResource(int kafkaOrdinal) {
        this.kafkaOrdinal = kafkaOrdinal;
    }
}
