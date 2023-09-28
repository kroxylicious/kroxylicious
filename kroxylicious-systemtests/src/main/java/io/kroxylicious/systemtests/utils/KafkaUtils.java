/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.executor.ExecResult;

public class KafkaUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

    public static ExecResult ConsumeMessage(String topicName, int timeoutMilliseconds) {
        LOGGER.debug("Consuming messages from '{}' topic", topicName);
        return Exec.exec("kubectl", "-n", "kafka", "run", "java-kafka-consumer", "-i", "--image=" + Constants.STRIMZI_KAFKA_IMAGE, "--rm=true",
                "--restart=Never", "--", "bin/kafka-console-consumer.sh", "--bootstrap-server", Constants.KROXY_BOOTSTRAP, "--topic", topicName,
                "--from-beginning", "--timeout-ms", String.valueOf(timeoutMilliseconds));
    }

    public static ExecResult ProduceMessage(String topicName, String message) {
        LOGGER.debug("Sending '{}' message to '{}' topic", message, topicName);
        List<String> commands = SetCommands("kubectl", "-n", "kafka", "run", "java-kafka-producer", "-i",
                "--image=" + Constants.STRIMZI_KAFKA_IMAGE,
                "--rm=true", "--restart=Never", "--", "bin/kafka-console-producer.sh", "--bootstrap-server", Constants.KROXY_BOOTSTRAP,
                "--topic", topicName);
        // Produce
        return Exec.exec(message, commands);
    }

    public static List<String> SetCommands(String... command) {
        return Arrays.asList(command);
    }
}
