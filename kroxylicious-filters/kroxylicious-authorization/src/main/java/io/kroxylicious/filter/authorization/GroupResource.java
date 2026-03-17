/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Set;

import io.kroxylicious.authorizer.service.ResourceType;

public enum GroupResource implements ResourceType<GroupResource>, AuditAct {
    READ(3, "Read"),
    DELETE(6, "Delete"),
    DESCRIBE(8, "Describe"),
    DESCRIBE_CONFIGS(10, "DescribeConfigs"),
    ALTER_CONFIGS(11, "AlterConfigs");

    public final int kafkaOrdinal;
    private final String auditAction;

    GroupResource(int kafkaOrdinal, String auditAction) {
        this.kafkaOrdinal = kafkaOrdinal;
        this.auditAction = auditAction;
    }

    @Override
    public Set<GroupResource> implies() {
        return switch (this) {
            case READ, DELETE -> Set.of(DESCRIBE);
            case ALTER_CONFIGS -> Set.of(DESCRIBE_CONFIGS);
            default -> Set.of();
        };
    }

    @Override
    public String auditAction() {
        return auditAction;
    }
}
