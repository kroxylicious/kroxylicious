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
    private static KafClient kafClient;
    private static KcatClient kcatClient;
    private static StrimziTestClient strimziTestClient;

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
            default -> strimziTestClient();
        };
    }

    /**
     * Kaf client.
     *
     * @return the kaf client
     */
    public static KafClient kaf() {
        if (kafClient == null) {
            kafClient = new KafClient();
        }
        return kafClient;
    }

    /**
     * Strimzi test client.
     *
     * @return the Strimzi test client
     */
    public static StrimziTestClient strimziTestClient() {
        if (strimziTestClient == null) {
            strimziTestClient = new StrimziTestClient();
        }
        return strimziTestClient;
    }

    /**
     * Kcat client.
     *
     * @return the kcat client
     */
    public static KcatClient kcat() {
        if (kcatClient == null) {
            kcatClient = new KcatClient();
        }
        return kcatClient;
    }
}
