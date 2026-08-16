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
    /** Create resources (such as topics) on the cluster. */
    CREATE(0),
    /** Alter the cluster. */
    ALTER(1),
    /** Describe the cluster. */
    DESCRIBE(2),
    /** Perform inter-broker actions on the cluster. */
    CLUSTER_ACTION(3),
    /** Describe the configuration of the cluster. */
    DESCRIBE_CONFIGS(4),
    /** Alter the configuration of the cluster. */
    ALTER_CONFIGS(5),
    /** Connect to the cluster. This operation has no equivalent in Kafka's own authorization model. */
    CONNECT(-1);

    /** The bit index used for this operation when computing an {@code authorizedOperations} bitmask, or -1 if the operation has no Kafka equivalent. */
    public final int kafkaOrdinal;

    ClusterResource(int kafkaOrdinal) {
        this.kafkaOrdinal = kafkaOrdinal;
    }

}
