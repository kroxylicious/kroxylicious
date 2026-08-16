/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Set;

import io.kroxylicious.authorizer.service.ResourceType;

/**
 * The operations which may be performed on a Kafka consumer group.
 */
public enum GroupResource implements ResourceType<GroupResource> {
    /** Participate in the group (for example join it, or commit offsets to it). */
    READ(3),
    /** Delete the group. */
    DELETE(6),
    /** Describe the group. */
    DESCRIBE(8),
    /** Describe the configuration of the group. */
    DESCRIBE_CONFIGS(10),
    /** Alter the configuration of the group. */
    ALTER_CONFIGS(11);

    /** The code used by Kafka to identify the equivalent {@code AclOperation}. */
    public final int kafkaOrdinal;

    GroupResource(int kafkaOrdinal) {
        this.kafkaOrdinal = kafkaOrdinal;
    }

    @Override
    public Set<GroupResource> implies() {
        return switch (this) {
            case READ, DELETE -> Set.of(DESCRIBE);
            case ALTER_CONFIGS -> Set.of(DESCRIBE_CONFIGS);
            default -> Set.of();
        };
    }
}
