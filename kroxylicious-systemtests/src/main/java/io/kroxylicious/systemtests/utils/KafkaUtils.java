/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.strimzi.KafkaType;

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
    }

    /**
     * Produce messages with cmd.
     *
     * @param deployNamespace the deploy namespace
     * @param executableCommand the executable command
     * @param message the message
     * @param podName the pod name
     * @param clientName the client name
     */
    public static void produceMessagesWithCmd(String deployNamespace, List<String> executableCommand, String message, String podName, String clientName) {
        LOGGER.atInfo().setMessage("Executing command: {} for running {} producer").addArgument(executableCommand).addArgument(clientName).log();
        ExecResult result = Exec.exec(String.valueOf(message), executableCommand, Duration.ofSeconds(30), true, false, null);

        String log = kubeClient().logsInSpecificNamespace(deployNamespace, podName);
        if (result.isSuccess()) {
            LOGGER.atInfo().setMessage("{} client produce log: {}").addArgument(clientName).addArgument(log).log();
            deletePod(deployNamespace, podName);
        }
        else {
            LOGGER.atError().setMessage("error producing messages with {}: {}").addArgument(clientName).addArgument(log).log();
            throw new KubeClusterException("error producing messages with " + clientName + ": " + log);
        }
    }

    /**
     * Create job
     *
     * @param namespace the namespace
     * @param name the name
     * @param clientJob the client job
     * @return the Pod Name
     */
    public static String createJob(String namespace, String name, Job clientJob) {
        kubeClient().getClient().batch().v1().jobs().inNamespace(namespace).resource(clientJob).create();
        return KafkaUtils.getPodNameByLabel(namespace, "app", name, Duration.ofSeconds(30));
    }

    /**
     * Delete job
     *
     * @param clientJob the client job
     */
    public static void deleteJob(Job clientJob) {
        kubeClient().getClient().batch().v1().jobs().resource(clientJob).withTimeout(30, TimeUnit.SECONDS).delete();
    }

    /**
     * Delete a pod
     *
     * @param namespace namespace
     * @param podName pod name
     */
    private static void deletePod(String namespace, String podName) {
        kubeClient().getClient().pods().inNamespace(namespace).withName(podName).withTimeout(60, TimeUnit.SECONDS).delete();
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
        DeploymentUtils.waitForDeploymentRunning(deployNamespace, podName, Duration.ofMinutes(5));
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

    /**
     * Gets kafka listener status.
     *
     * @param listenerStatusName the listener status name
     * @return the kafka listener status
     */
    public static Optional<ListenerStatus> getKafkaListenerStatus(String listenerStatusName) {
        KafkaType type = new KafkaType();
        return await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(200))
                .until(
                        () -> type.getClient().inNamespace(Constants.KAFKA_DEFAULT_NAMESPACE)
                                .list()
                                .getItems()
                                .stream()
                                .findFirst()
                                .map(Kafka::getStatus)
                                .map(KafkaStatus::getListeners)
                                .stream()
                                .flatMap(List::stream)
                                .filter(listenerStatus -> listenerStatus.getName().contains(listenerStatusName))
                                .findFirst(),
                        Optional::isPresent);
    }

    /**
     * Is kafka up.
     *
     * @param clusterName the cluster name
     * @return the boolean
     */
    public static boolean isKafkaUp(String clusterName) {
        List<Pod> kafkaPods = kubeClient().listPods(Constants.KAFKA_DEFAULT_NAMESPACE).stream()
                .filter(pod -> pod.getMetadata().getName().contains(clusterName))
                .toList();

        return !kafkaPods.isEmpty();
    }
}
