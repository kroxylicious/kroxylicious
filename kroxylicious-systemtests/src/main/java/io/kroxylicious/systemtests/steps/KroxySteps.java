/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.io.FileNotFoundException;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.utils.KafkaUtils;

public class KroxySteps {

    public static void produceMessages(String topicName, String message, int numberOfMessages) throws FileNotFoundException {
        KafkaUtils.ProduceMessageFromYaml(Constants.KROXY_DEFAULT_NAMESPACE, topicName, message, Constants.KROXY_BOOTSTRAP);
    }

    public static String consumeMessages(String topicName, int numberOfMessages, long timoutMillis) throws FileNotFoundException {
        return KafkaUtils.ConsumeMessageFromYaml(Constants.KROXY_DEFAULT_NAMESPACE, topicName, Constants.KROXY_BOOTSTRAP, numberOfMessages, timoutMillis);
    }
}
