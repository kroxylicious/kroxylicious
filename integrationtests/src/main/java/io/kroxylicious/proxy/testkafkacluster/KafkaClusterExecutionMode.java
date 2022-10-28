/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testkafkacluster;

public enum KafkaClusterExecutionMode {
    /**
     * Kafka/Zookeeper will be run within the same JVM as the caller
     */
    IN_VM,
    /**
     * Kafka/Zookeeper will be run in containers
     */
    CONTAINER;

    public static KafkaClusterExecutionMode convertClusterExecutionMode(String mode, KafkaClusterExecutionMode defaultMode) {
        try {
            if (mode == null) {
                return defaultMode;
            }
            return valueOf(KafkaClusterExecutionMode.class, mode.toUpperCase());
        } catch (IllegalArgumentException e) {
            return defaultMode;
        }
    }
}
