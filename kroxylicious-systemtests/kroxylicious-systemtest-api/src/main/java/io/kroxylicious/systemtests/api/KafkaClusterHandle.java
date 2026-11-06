/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api;

/**
 * A token proving that an upstream Kafka cluster has been provisioned and is ready.
 * Obtained exclusively from {@link KafkaClusterFixture#provision()}.
 */
public interface KafkaClusterHandle {

    /**
     * Returns the bootstrap server address of the provisioned Kafka cluster.
     *
     * @return bootstrap address (host:port)
     */
    String bootstrap();
}
