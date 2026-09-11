/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Set;

import io.kroxylicious.authorizer.service.ResourceType;

/**
 * The operations which may be performed on a Kafka topic.
 */
public enum TopicResource implements ResourceType<TopicResource> {
    /** Read records from the topic. */
    READ(3),
    /** Write records to the topic. */
    WRITE(4),
    /** Create the topic. */
    CREATE(5),
    /** Delete the topic. */
    DELETE(6),
    /** Alter the topic (for example its number of partitions). */
    ALTER(7),
    /** Describe the topic. */
    DESCRIBE(8),
    /** Describe the configuration of the topic. */
    DESCRIBE_CONFIGS(10),
    /** Alter the configuration of the topic. */
    ALTER_CONFIGS(11);

    /** The code used by Kafka to identify the equivalent {@code AclOperation}. */
    public final int kafkaOrdinal;

    TopicResource(int kafkaOrdinal) {
        this.kafkaOrdinal = kafkaOrdinal;
    }

    @Override
    public Set<TopicResource> implies() {
        return switch (this) {
            case READ, WRITE, DELETE, ALTER -> Set.of(DESCRIBE);
            case ALTER_CONFIGS -> Set.of(DESCRIBE_CONFIGS);
            default -> Set.of();
        };
    }
}
