/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClientException;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.templates.testclients.TestClientsJobTemplates;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The Kafka utils.
 */
public class KafkaUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

    private KafkaUtils() {
    }

    /**
     * Produce messages.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param name the name
     * @param clientJob the client job
     */
    public static void produceMessages(String deployNamespace, String topicName, String name, Job clientJob) {
        LOGGER.atInfo().setMessage("Producing messages in '{}' topic").addArgument(topicName).log();
        kubeClient().getClient().batch().v1().jobs().inNamespace(deployNamespace).resource(clientJob).create();
        String podName = KafkaUtils.getPodNameByLabel(deployNamespace, "app", name, Duration.ofSeconds(30));
        DeploymentUtils.waitForDeploymentRunning(deployNamespace, podName, Duration.ofSeconds(30));
        LOGGER.atInfo().setMessage("client producer log: {}").addArgument(kubeClient().logsInSpecificNamespace(deployNamespace, podName)).log();
    }

    /**
     * Consume messages.
     *
     * @param topicName the topic name
     * @param name the name
     * @param namespace the deploy namespace
     * @param clientJob the client job
     * @param message the message
     * @param numOfMessages the num of messages
     * @param timeout the timeout
     * @return the string
     */
    public static String consumeMessages(String topicName, String name, String namespace, Job clientJob, String message, int numOfMessages, Duration timeout) {
        LOGGER.atInfo().setMessage("Consuming messages from '{}' topic").addArgument(topicName).log();
        kubeClient().getClient().batch().v1().jobs().inNamespace(namespace).resource(clientJob).create();
        String podName = KafkaUtils.getPodNameByLabel(namespace, "app", name, Duration.ofSeconds(30));
        String log;
        try{
            log = await().alias("Consumer waiting to receive messages")
                    .ignoreException(KubernetesClientException.class)
                    .atMost(timeout)
                    .until(() -> {
                        if (kubeClient().getClient().pods().inNamespace(namespace).withName(podName).get() != null) {
                            return kubeClient().logsInSpecificNamespace(namespace, podName);
                        }
                        return null;
                    }, m -> StringUtils.countMatches(m, message) == (numOfMessages));
        }
        catch (ConditionTimeoutException e) {
            log = kubeClient().logsInSpecificNamespace(namespace, podName);
            LOGGER.atInfo().setMessage("Timeout! Received: {}").addArgument(log).log();
        }
        return log;
    }

    /**
     * Consume message with test clients.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numOfMessages the num of messages
     * @param timeout the timeout
     * @return the log of the pod
     */
    public static String consumeMessageWithTestClients(String deployNamespace, String topicName, String bootstrap, String message, int numOfMessages, Duration timeout) {
        String name = Constants.KAFKA_CONSUMER_CLIENT_LABEL;
        Job testClientJob = TestClientsJobTemplates.defaultTestClientConsumerJob(name, bootstrap, topicName, numOfMessages).build();
        return consumeMessages(topicName, name, deployNamespace, testClientJob, message, numOfMessages, timeout);
    }

    /**
     * Gets pod name by label.
     *
     * @param deployNamespace the deploy namespace
     * @param labelKey the label key
     * @param labelValue the label value
     * @param timeout the timeout
     * @return the pod name by label
     */
    public static String getPodNameByLabel(String deployNamespace, String labelKey, String labelValue, Duration timeout) {
        List<Pod> pods = await().atMost(timeout).until(() -> kubeClient().listPods(deployNamespace, labelKey, labelValue),
                p -> !p.isEmpty());
        return pods.get(pods.size() - 1).getMetadata().getName();
    }

    /**
     * Produce message with test clients.
     *
     * @param deployNamespace the deploy namespace
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param message the message
     * @param numOfMessages the num of messages
     */
    public static void produceMessageWithTestClients(String deployNamespace, String topicName, String bootstrap, String message, int numOfMessages) {
        String name = Constants.KAFKA_PRODUCER_CLIENT_LABEL;
        Job testClientJob = TestClientsJobTemplates.defaultTestClientProducerJob(name, bootstrap, topicName, numOfMessages, message).build();
        produceMessages(deployNamespace, topicName, name, testClientJob);
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
        List<Pod> kafkaPods = kubeClient().listPods(Constants.KAFKA_DEFAULT_NAMESPACE);
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
