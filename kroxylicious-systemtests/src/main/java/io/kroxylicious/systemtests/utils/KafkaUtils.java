/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

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
     * Consume message with test clients.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numOfMessages the num of messages
     * @param timeoutMilliseconds the timeout milliseconds
     * @return the string
     * @throws IOException the io exception
     */
    public static String ConsumeMessageWithTestClients(String deployNamespace, String topicName, String bootstrap, int numOfMessages, long timeoutMilliseconds)
            throws IOException {

        LOGGER.debug("Consuming messages from '{}' topic", topicName);
        File file = replaceStringInResourceFile("kafka-consumer-template.yaml", "kafka-consumer.yaml", Map.of(
                "%BOOTSTRAP_SERVERS%", bootstrap,
                "%TOPIC_NAME%", topicName,
                "%MESSAGE_COUNT%", "\"" + numOfMessages + "\""));

        kubeClient().getClient().load(new FileInputStream(file)).inNamespace(deployNamespace).create();
        String podName = getPodNameByLabel(deployNamespace, "app", Constants.KAFKA_CONSUMER_CLIENT_LABEL, timeoutMilliseconds);
        TestUtils.waitFor("", 1000, timeoutMilliseconds,
                () -> {
                    var log = kubeClient().getClient().pods().inNamespace(deployNamespace).withName(podName).getLog();
                    return log.contains(" - " + (numOfMessages - 1));
                });
        return kubeClient().getClient().pods().inNamespace(deployNamespace).withName(podName).getLog();
    }

    private static String getPodNameByLabel(String deployNamespace, String labelKey, String labelValue, long timeoutMilliseconds) {
        TestUtils.waitFor("Waiting for Pod with label " + labelKey + "=" + labelValue + " to appear", 1000, timeoutMilliseconds,
                () -> {
                    var podList = kubeClient().getClient().pods().inNamespace(deployNamespace).withLabel(labelKey, labelValue);
                    return !podList.list().getItems().isEmpty();
                });
        var pods = kubeClient().getClient().pods().inNamespace(deployNamespace).withLabel(labelKey, labelValue);
        return pods.list().getItems().get(pods.list().getItems().size() - 1).getMetadata().getName();
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
     * Produce message with test clients.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numOfMessages the num of messages
     * @return the string
     * @throws IOException the io exception
     */
    public static String produceMessageWithTestClients(String deployNamespace, String topicName, String bootstrap, String message, int numOfMessages) throws IOException {
        LOGGER.debug("Producing {} messages in '{}' topic", numOfMessages, topicName);
        File file = replaceStringInResourceFile("kafka-producer-template.yaml", "kafka-producer.yaml", Map.of(
                "%BOOTSTRAP_SERVERS%", bootstrap,
                "%TOPIC_NAME%", topicName,
                "%MESSAGE_COUNT%", "\"" + numOfMessages + "\"",
                "%MESSAGE%", message));
        kubeClient().getClient().load(new FileInputStream(file)).inNamespace(deployNamespace).create();
        return getPodNameByLabel(deployNamespace, "app", Constants.KAFKA_PRODUCER_CLIENT_LABEL, Duration.ofSeconds(10).toMillis());
    }

    private static File replaceStringInResourceFile(String resourceTemplateFileName, String newResourceFileName, Map<String, String> replacements) throws IOException {
        Path path = Path.of(Objects.requireNonNull(KafkaUtils.class
                .getClassLoader().getResource(resourceTemplateFileName)).getPath());
        Charset charset = StandardCharsets.UTF_8;

        String content = new String(Files.readAllBytes(path), charset);
        for (Map.Entry<String, String> entry : replacements.entrySet()) {
            content = content.replaceAll(entry.getKey(), entry.getValue());
        }
        String resourceFile = "/tmp/" + newResourceFileName;

        Files.writeString(Path.of(resourceFile), content, charset);
        File file = new File(resourceFile);
        file.deleteOnExit();
        return file;
    }

    /**
     * Delete all jobs.
     *
     * @param deployNamespace the deploy namespace
     */
    public static void deleteAllJobs(String deployNamespace) {
        LOGGER.info("Deleting producer and consumer jobs in {} namespace", deployNamespace);
        kubeClient().getClient().batch().v1().jobs().inNamespace(deployNamespace).delete();
        kubeClient().getClient().pods().inNamespace(deployNamespace).withLabel("app", Constants.KAFKA_PRODUCER_CLIENT_LABEL).delete();
        kubeClient().getClient().pods().inNamespace(deployNamespace).withLabel("app", Constants.KAFKA_CONSUMER_CLIENT_LABEL).delete();
        kubeClient().getClient().pods().inNamespace(deployNamespace).withLabel("app", Constants.KAFKA_CONSUMER_CLIENT_LABEL).waitUntilCondition(x -> x == null, 10,
                TimeUnit.SECONDS);
    }

    /**
     * Restart broker boolean.
     *
     * @param deployNamespace the deploy namespace
     * @param clusterName the cluster name
     * @return the boolean
     */
    public static boolean restartBroker(String deployNamespace, String clusterName) {
        String podName = "";
        List<Pod> kafkaPods = kubeClient().getClient().pods().inNamespace(Constants.KROXY_DEFAULT_NAMESPACE).list().getItems();
        int numOfBrokers = kafkaPods.size();
        for (Pod pod : kafkaPods) {
            String tmpName = pod.getMetadata().getName();
            if (tmpName.startsWith(clusterName)) {
                podName = pod.getMetadata().getName();
            }
        }
        kubeClient().getClient().pods().inNamespace(deployNamespace).withName(podName).delete();
        kubeClient().getClient().pods().inNamespace(deployNamespace).withName(podName).waitUntilCondition(Objects::isNull, 10, TimeUnit.SECONDS);
        return numOfBrokers - 1 == kubeClient().getClient().pods().inNamespace(deployNamespace).list().getItems().size();
    }
}
