/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients;

import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.enums.KafkaClientType;

/**
 * The type Kafka clients.
 */
public class KafkaClients {

    private KafkaClients() {
    }

    /**
     * Gets kafka client.
     *
     * @return the kafka client
     */
    public static KafkaClient getKafkaClient() {
        return switch (Enum.valueOf(KafkaClientType.class, Environment.KAFKA_CLIENT.toUpperCase())) {
            case KAF -> kaf();
            case KCAT -> kcat();
            case PYTHON_TEST_CLIENT -> python();
            default -> strimziTestClient();
        };
    }

    /**
     * Kaf client.
     *
     * @return the kaf client
     */
    public static KafClient kaf() {
        return new KafClient();
    }

    public static PythonClient python() {
        return new PythonClient();
    }

    /**
     * Strimzi test client.
     *
     * @return the Strimzi test client
     */
    public static StrimziTestClient strimziTestClient() {
        return new StrimziTestClient();
    }

    /**
     * Kcat client.
     *
     * @return the kcat client
     */
    public static KcatClient kcat() {
        return new KcatClient();
    }
}
