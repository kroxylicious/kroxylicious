/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;

import io.kroxylicious.systemtests.Constants;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Kafka utils.
 */
public class KafkaUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

    /**
     * Consume message string.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param timeoutMilliseconds the timeout milliseconds
     * @return the string
     */
    public static String ConsumeMessage(String deployNamespace, String topicName, String bootstrap, int timeoutMilliseconds) {
        LOGGER.debug("Consuming messages from '{}' topic", topicName);

        Pod pod = kubeClient().getClient().run().inNamespace(deployNamespace).withNewRunConfig()
                .withImage(Constants.STRIMZI_KAFKA_IMAGE)
                .withName("java-kafka-consumer")
                .withRestartPolicy("Never")
                .withCommand("/bin/sh")
                .withArgs("-c",
                        "bin/kafka-console-consumer.sh --bootstrap-server " + bootstrap + " --topic " + topicName + " --from-beginning --timeout-ms "
                                + timeoutMilliseconds)
                .done();
        long deadline = System.currentTimeMillis() + timeoutMilliseconds * 2L;
        long timeLeft = deadline;
        while (timeLeft > 0) {
            try {
                Thread.sleep(1000);
                timeLeft = deadline - System.currentTimeMillis();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.trace(e.getMessage());
            }
        }
        String log = kubeClient().getClient().pods().inNamespace(deployNamespace).withName("java-kafka-consumer").getLog();
        kubeClient().getClient().pods().inNamespace(deployNamespace).withName("java-kafka-consumer").delete();
        return log;
    }

    /**
     * Consume message from yaml string.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numOfMessages the num of messages
     * @param timeoutMilliseconds the timeout milliseconds
     * @return the string
     * @throws FileNotFoundException the file not found exception
     */
    public static String ConsumeMessageFromYaml(String deployNamespace, String topicName, String bootstrap, int numOfMessages, int timeoutMilliseconds)
            throws FileNotFoundException {
        LOGGER.debug("Consuming messages from '{}' topic", topicName);

        File file = new File(Objects.requireNonNull(KafkaUtils.class
                .getClassLoader().getResource("kafka-consumer.yaml")).getFile());
        kubeClient().getClient().load(new FileInputStream(file)).inNamespace(deployNamespace).create();
        String podName = getPodNameByLabel(deployNamespace, "app", "kafka-consumer-client", timeoutMilliseconds);
        TestUtils.waitFor("", 1000, timeoutMilliseconds,
                () -> {
                    var log = kubeClient().getClient().pods().inNamespace(deployNamespace).withName(podName).getLog();
                    return log.contains(" - " + (numOfMessages - 1));
                });
        return kubeClient().getClient().pods().inNamespace(deployNamespace).withName(podName).getLog();
    }

    private static String getPodNameByLabel(String deployNamespace, String labelKey, String labelValue, int timeoutMilliseconds) {
        TestUtils.waitFor("Waiting for Pod with label " + labelKey + "=" + labelValue + " to appear", 1000, timeoutMilliseconds,
                () -> {
                    var podList = kubeClient().getClient().pods().inNamespace(deployNamespace).withLabel(labelKey, labelValue);
                    return !podList.list().getItems().isEmpty();
                });
        var pods = kubeClient().getClient().pods().inNamespace(deployNamespace).withLabel(labelKey, labelValue);
        return pods.list().getItems().get(0).getMetadata().getName();
    }

    /**
     * Produce message.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param message the message
     * @param bootstrap the bootstrap
     */
    public static void ProduceMessage(String deployNamespace, String topicName, String message, String bootstrap) {
        kubeClient().getClient().run().inNamespace(deployNamespace).withNewRunConfig()
                .withImage(Constants.STRIMZI_KAFKA_IMAGE)
                .withName("java-kafka-producer")
                .withRestartPolicy("Never")
                .withCommand("/bin/sh")
                .withArgs("-c", "echo '" + message + "'| bin/kafka-console-producer.sh --bootstrap-server " + bootstrap + " --topic " + topicName)
                .done();
    }

    /**
     * Produce message from yaml.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param message the message
     * @param bootstrap the bootstrap
     * @throws FileNotFoundException the file not found exception
     */
    public static void ProduceMessageFromYaml(String deployNamespace, String topicName, String message, String bootstrap) throws FileNotFoundException {
        LOGGER.debug("Producing messages in '{}' topic", topicName);
        File file = new File(Objects.requireNonNull(KafkaUtils.class
                .getClassLoader().getResource("kafka-producer.yaml")).getFile());
        kubeClient().getClient().load(new FileInputStream(file)).inNamespace(deployNamespace).create();
        // kubeClient().getClient().apps().deployments().inNamespace(deployNamespace).withName("kafka-producer-client").waitUntilReady(10, TimeUnit.SECONDS);
    }
}
