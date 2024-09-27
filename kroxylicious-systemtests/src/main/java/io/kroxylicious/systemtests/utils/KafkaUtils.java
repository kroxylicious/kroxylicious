/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

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
        DeploymentUtils.waitForDeploymentToBeRestarted(deployNamespace, podName, Duration.ofMinutes(5));
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
