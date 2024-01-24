/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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
import io.fabric8.kubernetes.client.KubernetesClientException;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The Kafka utils.
 */
public class KafkaUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);
    private static final String BOOTSTRAP_SERVERS_VAR = "%BOOTSTRAP_SERVERS%";
    private static final String NAMESPACE_VAR = "%NAMESPACE%";
    private static final String TOPIC_NAME_VAR = "%TOPIC_NAME%";
    private static final String MESSAGE_COUNT_VAR = "%MESSAGE_COUNT%";
    private static final String MESSAGE_VAR = "%MESSAGE%";
    private static final String KAFKA_VERSION_VAR = "%KAFKA_VERSION%";

    /**
     * Consume message with java client.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param timeout the timeout
     * @return the log of the pod
     */
    public static String consumeMessageWithJavaClient(String deployNamespace, String topicName, String bootstrap, Duration timeout) {
        LOGGER.debug("Consuming messages from '{}' topic", topicName);

        String kafkaConsumerName = "java-kafka-consumer";
        Pod pod = kubeClient().getClient().run().inNamespace(deployNamespace).withNewRunConfig()
                .withImage(Constants.STRIMZI_KAFKA_IMAGE)
                .withName(kafkaConsumerName)
                .withRestartPolicy("Never")
                .withCommand("/bin/sh")
                .withArgs("-c",
                        "bin/kafka-console-consumer.sh --bootstrap-server " + bootstrap + " --topic " + topicName + " --from-beginning --timeout-ms "
                                + timeout.toMillis())
                .done();
        long deadline = System.currentTimeMillis() + timeout.toMillis() * 2L;
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
        String log = kubeClient().logsInSpecificNamespace(deployNamespace, kafkaConsumerName);
        kubeClient().getClient().pods().inNamespace(deployNamespace).withName(kafkaConsumerName).delete();
        return log;
    }

    private static String consumeMessages(String deployNamespace, String topicName, String bootstrap, int numOfMessages, String messageToSeek, Duration timeout) {
        LOGGER.debug("Consuming messages from '{}' topic", topicName);
        InputStream file = replaceStringInResourceFile("kafka-consumer-template.yaml", Map.of(
                BOOTSTRAP_SERVERS_VAR, bootstrap,
                NAMESPACE_VAR, deployNamespace,
                TOPIC_NAME_VAR, topicName,
                MESSAGE_COUNT_VAR, "\"" + numOfMessages + "\"",
                KAFKA_VERSION_VAR, Environment.KAFKA_VERSION));

        kubeClient().getClient().load(file).inNamespace(deployNamespace).create();
        String podName = getPodNameByLabel(deployNamespace, "app", Constants.KAFKA_CONSUMER_CLIENT_LABEL, timeout);
        await().alias("Consumer waiting to receive messages").ignoreException(KubernetesClientException.class).atMost(timeout)
                .until(() -> {
                    if (kubeClient().getClient().pods().inNamespace(deployNamespace).withName(podName).get() != null) {
                        var log = kubeClient().logsInSpecificNamespace(deployNamespace, podName);
                        return log.contains(messageToSeek);
                    }
                    return false;
                });
        return kubeClient().logsInSpecificNamespace(deployNamespace, podName);
    }

    /**
     * Consume message with test clients.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numOfMessages the num of messages
     * @param timeout the timeout
     * @return the log of the pod
     */
    public static String consumeMessageWithTestClients(String deployNamespace, String topicName, String bootstrap, int numOfMessages, Duration timeout) {
        return consumeMessages(deployNamespace, topicName, bootstrap, numOfMessages, " - " + (numOfMessages - 1), timeout);
    }

    /**
     * Consume encrypted message with test clients.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param numOfMessages the num of messages
     * @param timeout the timeout
     * @return the string
     */
    public static String consumeEncryptedMessageWithTestClients(String deployNamespace, String topicName, String bootstrap, int numOfMessages, Duration timeout) {
        return consumeMessages(deployNamespace, topicName, bootstrap, numOfMessages, "key: kroxylicious.io/encryption", timeout);
    }

    private static String getPodNameByLabel(String deployNamespace, String labelKey, String labelValue, Duration timeout) {
        await().atMost(timeout).until(() -> {
            var podList = kubeClient().listPods(deployNamespace, labelKey, labelValue);
            return !podList.isEmpty();
        });
        var pods = kubeClient().listPods(deployNamespace, labelKey, labelValue);
        return pods.get(pods.size() - 1).getMetadata().getName();
    }

    /**
     * Produce message with Java Client.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param message the message
     * @param bootstrap the bootstrap
     */
    public static void produceMessageWithJavaClient(String deployNamespace, String topicName, String message, String bootstrap) {
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
     * @return the name of the pod
     */
    public static String produceMessageWithTestClients(String deployNamespace, String topicName, String bootstrap, String message, int numOfMessages) {
        LOGGER.debug("Producing {} messages in '{}' topic", numOfMessages, topicName);
        InputStream file = replaceStringInResourceFile("kafka-producer-template.yaml", Map.of(
                BOOTSTRAP_SERVERS_VAR, bootstrap,
                NAMESPACE_VAR, deployNamespace,
                TOPIC_NAME_VAR, topicName,
                MESSAGE_COUNT_VAR, "\"" + numOfMessages + "\"",
                MESSAGE_VAR, message,
                KAFKA_VERSION_VAR, Environment.KAFKA_VERSION));
        kubeClient().getClient().load(file).inNamespace(deployNamespace).create();
        return getPodNameByLabel(deployNamespace, "app", Constants.KAFKA_PRODUCER_CLIENT_LABEL, Duration.ofSeconds(10));
    }

    private static InputStream replaceStringInResourceFile(String resourceTemplateFileName, Map<String, String> replacements) {
        Path path = Path.of(Objects.requireNonNull(KafkaUtils.class
                .getClassLoader().getResource(resourceTemplateFileName)).getPath());
        Charset charset = StandardCharsets.UTF_8;

        String content;
        try {
            content = Files.readString(path, charset);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (Map.Entry<String, String> entry : replacements.entrySet()) {
            content = content.replaceAll(entry.getKey(), entry.getValue());
        }
        return new ByteArrayInputStream(content.getBytes());
    }

    /**
     * Restart broker
     *
     * @param deployNamespace the deploy namespace
     * @param clusterName the cluster name
     * @return true if the restart has been done successfully, false otherwise
     */
    public static boolean restartBroker(String deployNamespace, String clusterName) {
        String podName = "";
        String podUid = "";
        List<Pod> kafkaPods = kubeClient().listPods(Constants.KROXY_DEFAULT_NAMESPACE);
        for (Pod pod : kafkaPods) {
            String tmpName = pod.getMetadata().getName();
            if (tmpName.startsWith(clusterName) && tmpName.endsWith("0")) {
                podName = pod.getMetadata().getName();
                podUid = pod.getMetadata().getUid();
                break;
            }
        }
        if (podName.isEmpty() || podName.isBlank()) {
            throw new KubeClusterException.NotFound("Kafka cluster name not found!");
        }
        kubeClient().getClient().pods().inNamespace(deployNamespace).withName(podName).withGracePeriod(0).delete();
        kubeClient().getClient().pods().inNamespace(deployNamespace).withName(podName).waitUntilCondition(Objects::isNull, 60, TimeUnit.SECONDS);
        String finalPodName = podName;
        await().atMost(Duration.ofMinutes(1)).until(() -> kubeClient().getClient().pods().inNamespace(deployNamespace).withName(finalPodName) != null);
        return !Objects.equals(podUid, getPodUid(deployNamespace, podName));
    }

    private static String getPodUid(String deployNamespace, String podName) {
        final Pod pod = kubeClient().getPod(deployNamespace, podName);
        if (pod != null) {
            return pod.getMetadata().getUid();
        }
        else {
            return "";
        }
    }
}
