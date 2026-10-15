/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api;

/**
 * Provisions an upstream Kafka cluster for the proxy to front.
 * Implementations handle the specific provisioning mechanism (TestContainers, Strimzi, in-VM, etc.).
 * <p>
 * The implementation is selected via the {@code KROXYLICIOUS_TEST_KAFKA_CLUSTER} environment variable
 * and injected by the JUnit extension at class scope.
 */
public interface KafkaClusterFixture {

    /**
     * Provisions a Kafka cluster and blocks until it is ready.
     *
     * @return a handle to the provisioned cluster
     */
    KafkaClusterHandle provision();
}
