/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testcluster;

public enum ClusterExecutionMode {
    /**
     * Kafka/Zookeeper will be run within the same JVM as the caller
     */
    IN_VM,
    /**
     * Kafka/Zookeeper will be run in containers
     */
    CONTAINER;

    public static ClusterExecutionMode convertClusterExecutionMode(String mode, ClusterExecutionMode defaultMode) {
        try {
            if (mode == null) {
                return defaultMode;
            }
            return valueOf(ClusterExecutionMode.class, mode.toUpperCase());
        } catch (IllegalArgumentException e) {
            return defaultMode;
        }
    }
}
