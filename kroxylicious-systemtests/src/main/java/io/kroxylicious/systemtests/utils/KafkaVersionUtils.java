/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

/**
 * The type Kafka version utils.
 */
public class KafkaVersionUtils {

    private KafkaVersionUtils() {
    }

    /**
     * Gets kafka protocol version.
     *
     * @param kafkaVersion the kafka version
     * @return the kafka protocol version
     */
    public static String getKafkaProtocolVersion(String kafkaVersion) {
        String[] splitVersion = kafkaVersion.split("\\.");

        return String.format("%s.%s", splitVersion[0], splitVersion[1]);
    }
}
