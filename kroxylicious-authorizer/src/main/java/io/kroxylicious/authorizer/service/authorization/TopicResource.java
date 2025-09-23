/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service.authorization;

import java.util.Set;

public enum TopicResource implements Operation<TopicResource> {
    DESCRIBE,
    READ,
    WRITE,
    CREATE,
    DELETE,
    ALTER,
    DESCRIBE_CONFIGS,
    ALTER_CONFIGS;

    @Override
    public Set<TopicResource> implies() {
        return switch (this) {
            case READ, WRITE, DELETE, ALTER -> Set.of(DESCRIBE);
            case ALTER_CONFIGS -> Set.of(DESCRIBE_CONFIGS);
            default -> Set.of();
        };
    }
}
