/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.enums;

public enum KafkaClientType {
    STRIMZI_TEST_CLIENT(true),
    KCAT(false),
    KAF(false),
    PYTHON_TEST_CLIENT(false);

    private final boolean jvmClient;

    KafkaClientType(boolean jvmClient) {
        this.jvmClient = jvmClient;
    }

    /**
     * Whether the client is based on the Apache Kafka Java client, and therefore understands
     * Java-only configuration properties such as {@code sasl.jaas.config}. Non-JVM clients
     * (librdkafka-based kcat/python, sarama-based kaf) reject such properties.
     *
     * @return true if the client uses the Apache Kafka Java client
     */
    public boolean isJvmClient() {
        return jvmClient;
    }
}
