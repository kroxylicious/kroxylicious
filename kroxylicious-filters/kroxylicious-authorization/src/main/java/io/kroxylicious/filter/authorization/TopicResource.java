/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Set;

import io.kroxylicious.authorizer.service.ResourceType;

public enum TopicResource implements ResourceType<TopicResource> {
    READ(3),
    WRITE(4),
    CREATE(5),
    DELETE(6),
    ALTER(7),
    DESCRIBE(8),
    DESCRIBE_CONFIGS(10),
    ALTER_CONFIGS(11);

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
