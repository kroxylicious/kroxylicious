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
public enum ClusterResource implements ResourceType<ClusterResource> {
    CREATE(0),
    ALTER(1),
    DESCRIBE(2),
    CLUSTER_ACTION(3),
    DESCRIBE_CONFIGS(4),
    ALTER_CONFIGS(5),
    CONNECT(-1);

    public final int kafkaOrdinal;

    ClusterResource(int kafkaOrdinal) {
        this.kafkaOrdinal = kafkaOrdinal;
    }

}
