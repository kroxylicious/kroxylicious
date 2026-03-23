/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import io.kroxylicious.authorizer.service.ResourceType;

/**
 * A resource representing a Kafka cluster
 */
public enum ClusterResource implements ResourceType<ClusterResource>, AuditAct {
    CREATE(0, "Create"),
    ALTER(1, "Alter"),
    DESCRIBE(2, "Describe"),
    CLUSTER_ACTION(3, "ClusterAction"),
    DESCRIBE_CONFIGS(4, "DescribeConfigs"),
    ALTER_CONFIGS(5, "AlterConfigs"),
    CONNECT(-1, "Connect"),;

    public final int kafkaOrdinal;
    private final String auditAction;

    ClusterResource(int kafkaOrdinal, String auditAction) {
        this.kafkaOrdinal = kafkaOrdinal;
        this.auditAction = auditAction;
    }

    @Override
    public String auditAction() {
        return auditAction;
    }

}