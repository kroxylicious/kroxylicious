/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl.allow;

import java.util.Set;

import io.kroxylicious.authorizer.service.ResourceType;

public enum FakeTopicResource implements ResourceType<FakeTopicResource> {
    DESCRIBE,
    CREATE,
    WRITE,
    ALTER,
    READ;

    @Override
    public Set<FakeTopicResource> implies() {
        return switch (this) {
            case READ, WRITE, ALTER -> Set.of(DESCRIBE);
            default -> Set.of();
        };
    }
}
