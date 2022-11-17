/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.kafkacluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaClusterFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterFactory.class);

    /**
     * environment variable specifying execution mode, IN_VM or CONTAINER.
     */
    public static final String TEST_CLUSTER_EXECUTION_MODE = "TEST_CLUSTER_EXECUTION_MODE";

    /**
     * environment variable specifying kraft mode, true or false.
     */
    public static final String TEST_CLUSTER_KRAFT_MODE = "TEST_CLUSTER_KRAFT_MODE";

    public static KafkaCluster create(KafkaClusterConfig clusterConfig) {
        if (clusterConfig == null) {
            throw new NullPointerException();
        }

        var clusterMode = KafkaClusterExecutionMode.convertClusterExecutionMode(System.getenv().get(TEST_CLUSTER_EXECUTION_MODE), KafkaClusterExecutionMode.IN_VM);
        var kraftMode = convertClusterKraftMode(System.getenv().get(TEST_CLUSTER_KRAFT_MODE), true);

        var builder = clusterConfig.toBuilder();

        if (clusterConfig.getExecMode() == null) {
            builder.execMode(clusterMode);
        }

        if (clusterConfig.getKraftMode() == null) {
            builder.kraftMode(kraftMode);
        }

        var actual = builder.build();
        LOGGER.info("Test cluster : {}", actual);

        if (actual.getExecMode() == KafkaClusterExecutionMode.IN_VM) {
            return new InVMKafkaCluster(actual);
        }
        else {
            return new ContainerBasedKafkaCluster(actual);
        }
    }

    private static boolean convertClusterKraftMode(String mode, boolean defaultMode) {
        if (mode == null) {
            return defaultMode;
        }
        return Boolean.parseBoolean(mode);
    }

}
