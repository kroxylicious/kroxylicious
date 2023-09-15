/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.utils;

public class KafkaVersionUtils {

    public static String getKafkaProtocolVersion(String kafkaVersion) {
        String[] splitVersion = kafkaVersion.split("\\.");

        return String.format("%s.%s", splitVersion[0], splitVersion[1]);
    }
}
