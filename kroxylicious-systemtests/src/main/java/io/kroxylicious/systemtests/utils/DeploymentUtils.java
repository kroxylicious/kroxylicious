/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;
import org.awaitility.core.ConditionEvaluationListener;
import org.awaitility.core.EvaluatedCondition;
import org.awaitility.core.TimeoutEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousSecretTemplates;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Deployment utils.
 */
public class DeploymentUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeploymentUtils.class);
    private static final Duration READINESS_TIMEOUT = Duration.ofMinutes(6);
    private static final Duration DELETION_TIMEOUT = Duration.ofMinutes(5);
    private static final String TEST_LOAD_BALANCER_NAME = "test-load-balancer";

    private DeploymentUtils() {
    }

    /**
     * Wait for deployment ready.
     *
     * @param namespaceName the namespace name
     * @param deploymentName the deployment name
     */
    public static void waitForDeploymentReady(String namespaceName, String deploymentName) {
        LOGGER.info("Waiting for Deployment: {}/{} to be ready", namespaceName, deploymentName);

        await().atMost(READINESS_TIMEOUT).pollInterval(Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS)
                .until(() -> kubeClient(namespaceName).isDeploymentReady(namespaceName, deploymentName));

        LOGGER.info("Deployment: {}/{} is ready", namespaceName, deploymentName);
    }

    /**
     * Wait until the given Deployment has been deleted.
     * @param namespaceName Namespace name
     * @param name The name of the Deployment.
     */
    public static void waitForDeploymentDeletion(String namespaceName, String name) {
        LOGGER.debug("Waiting for Deployment: {}/{} deletion", namespaceName, name);
        await().atMost(DELETION_TIMEOUT).pollInterval(Constants.POLL_INTERVAL_FOR_RESOURCE_DELETION)
                .until(() -> {
                    if (kubeClient(namespaceName).getDeployment(namespaceName, name) == null) {
                        return true;
                    }
                    else {
                        LOGGER.warn("Deployment: {}/{} is not deleted yet! Triggering force delete by cmd client!", namespaceName, name);
                        cmdKubeClient(namespaceName).deleteByName(Constants.DEPLOYMENT, name);
                        return false;
                    }
                });
        LOGGER.debug("Deployment: {}/{} was deleted", namespaceName, name);
    }

    /**
     * Gets deployment file from url.
     *
     * @param url the url
     * @return the deployment file from url
     * @throws IOException the io exception
     */
    public static FileInputStream getDeploymentFileFromURL(String url) throws IOException {
        File deploymentFile = Files.createTempFile("deploy", ".yaml", TestUtils.getDefaultPosixFilePermissions()).toFile();
        FileUtils.copyURLToFile(
                URI.create(url).toURL(),
                deploymentFile,
                5000,
                10000);
        deploymentFile.deleteOnExit();

        return new FileInputStream(deploymentFile);
    }

    /**
     * Check load balancer is working.
     *
     * @param namespace the namespace
     * @return the boolean
     */
    public static boolean checkLoadBalancerIsWorking(String namespace) {
        Service service = new ServiceBuilder()
                .withKind(Constants.SERVICE_KIND)
                .withNewMetadata()
                .withName(TEST_LOAD_BALANCER_NAME)
                .withNamespace(namespace)
                .addToLabels("app", "loadbalancer")
                .endMetadata()
                .withNewSpec()
                .addNewPort()
                .withPort(8080)
                .endPort()
                .withSelector(Collections.singletonMap("app", "loadbalancer"))
                .withType(Constants.LOAD_BALANCER_TYPE)
                .endSpec()
                .build();
        kubeClient().getClient().services().inNamespace(namespace).resource(service).create();
        boolean isWorking;
        try {
            LOGGER.debug("Waiting for the ingress IP to be available...");
            await().atMost(Duration.ofSeconds(10))
                    .until(() -> !kubeClient().getService(namespace, TEST_LOAD_BALANCER_NAME).getStatus().getLoadBalancer().getIngress().isEmpty()
                            && kubeClient().getService(namespace, TEST_LOAD_BALANCER_NAME).getStatus().getLoadBalancer().getIngress().get(0).getIp() != null);
            isWorking = true;
        }
        catch (Exception e) {
            isWorking = false;
            LOGGER.warn("The ingress IP is not available!");
            LOGGER.warn(e.getMessage());
        }
        kubeClient().getClient().apps().deployments().inNamespace(namespace).withName(TEST_LOAD_BALANCER_NAME).delete();
        return isWorking;
    }

    /**
     * Wait for deployment to be restarted.
     *
     * @param namespaceName the namespace name
     * @param podName the pod name
     * @param timeout the timeout
     */
    public static void waitForDeploymentToBeRestarted(String namespaceName, String podName, Duration timeout) {
        await().alias("await pod to be deleted or in non-running phase")
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> kubeClient().getPod(namespaceName, podName) == null
                        || !kubeClient().isDeploymentRunning(namespaceName, podName));
        DeploymentUtils.waitForDeploymentRunning(namespaceName, podName, timeout);
    }

    /**
     * Wait for deployment running.
     *
     * @param namespaceName the namespace name
     * @param podName the pod name
     * @param timeout the timeout
     */
    public static void waitForDeploymentRunning(String namespaceName, String podName, Duration timeout) {
        LOGGER.info("Waiting for deployment: {}/{} to be running", namespaceName, podName);
        waitForLeavingPendingPhase(namespaceName, podName);
        await().alias("await pod to be running or succeeded")
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> kubeClient().getPod(namespaceName, podName) != null
                        && kubeClient().isDeploymentRunning(namespaceName, podName));
    }

    private static void waitForLeavingPendingPhase(String namespaceName, String podName) {
        await().alias("await pod to leave pending phase")
                .atMost(Duration.ofMinutes(1))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> Optional.ofNullable(kubeClient().getPod(namespaceName, podName)).map(Pod::getStatus).map(PodStatus::getPhase),
                        s -> s.filter(Predicate.not(DeploymentUtils::isPendingPhase)).isPresent());
    }

    /**
     * Wait for run succeeded boolean.
     *
     * @param namespaceName the namespace name
     * @param podName the pod name
     * @param timeout the timeout
     */
    public static void waitForPodRunSucceeded(String namespaceName, String podName, Duration timeout) {
        LOGGER.info("Waiting for pod run: {}/{} to succeed", namespaceName, podName);

        waitForLeavingPendingPhase(namespaceName, podName);
        var pollInterval = 200;

        var terminalPhase = await().alias("await pod to reach terminal phase")
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(pollInterval))
                .conditionEvaluationListener(new TimeoutLoggingEvaluationListener(() -> kubeClient().logsInSpecificNamespace(namespaceName, podName)))
                .until(() -> Optional.of(kubeClient().getPod(namespaceName, podName)).map(Pod::getStatus).map(PodStatus::getPhase),
                        p -> p.map(DeploymentUtils::hasReachedTerminalPhase).orElse(false));

        if (!isSucceededPhase(terminalPhase.orElseThrow())) {
            LOGGER.atError().setMessage("Run failed! Error: {}").addArgument(() -> kubeClient().logsInSpecificNamespace(namespaceName, podName)).log();
            throw new KubeClusterException("Pod %s failed to execute".formatted(podName));
        }
    }

    private static boolean hasReachedTerminalPhase(String p) {
        return isFailedPhase(p) || isSucceededPhase(p);
    }

    private static boolean isSucceededPhase(String p) {
        return "succeeded".equalsIgnoreCase(p);
    }

    private static boolean isFailedPhase(String p) {
        return "failed".equalsIgnoreCase(p);
    }

    private static boolean isPendingPhase(String p) {
        LOGGER.atInfo().setMessage("Pod Status: {}").addArgument(p).log();
        return "pending".equalsIgnoreCase(p);
    }

    /**
     * Registry credentials secret.
     *
     * @param namespace the namespace
     */
    public static void registryCredentialsSecret(String namespace) {
        String configFolder = Environment.CONTAINER_CONFIG_PATH;
        SecretBuilder secretBuilder = KroxyliciousSecretTemplates.createRegistryCredentialsSecret(configFolder, namespace);
        if (secretBuilder != null) {
            LOGGER.atInfo().setMessage("Creating 'regcred' secret").log();
            Secret secret = secretBuilder.build();
            if (kubeClient().getClient().secrets().inNamespace(namespace).withName(secret.getMetadata().getName()).get() != null) {
                LOGGER.atInfo().setMessage("Skipping registry secret creation as it was already created").log();
                return;
            }
            kubeClient().getClient().secrets().inNamespace(namespace).resource(secret).create();
            await().atMost(Duration.ofSeconds(10)).until(() -> kubeClient().getClient().secrets().inNamespace(namespace).resource(secret).get() != null);
        }
    }

    /**
     * Collect cluster info.
     *
     * @param namespace the namespace
     * @param testClassName the test class name
     * @param testMethodName the test method name
     */
    public static void collectClusterInfo(String namespace, String testClassName, String testMethodName) {
        String formattedDate = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());
        List<String> executableCommand = List.of(cmdKubeClient(namespace).toString(), "cluster-info", "dump",
                "--namespaces", String.join(",", namespace, Constants.KAFKA_DEFAULT_NAMESPACE),
                "--output-directory", Environment.CLUSTER_DUMP_DIR + "/" + testClassName.replace(".", "_") + "_" +
                        testMethodName + "_cluster_dump_" + formattedDate);
        ExecResult result = Exec.exec(null, executableCommand, Duration.ofSeconds(20), true, false, null);
        if (result.isSuccess()) {
            LOGGER.atInfo().log(result.out());
        }
        else {
            throw new KubeClusterException("Unable to collect cluster info from namespaces '" + namespace + "' and '" + Constants.KAFKA_DEFAULT_NAMESPACE +
                    "': " + result.err());
        }
    }

    private record TimeoutLoggingEvaluationListener(Supplier<String> messageSupplier) implements ConditionEvaluationListener<PodStatus> {
        @Override
        public void onTimeout(TimeoutEvent timeoutEvent) {
            LOGGER.atError().setMessage("Timeout! Error: {}").addArgument(messageSupplier).log();
        }

        @Override
        public void conditionEvaluated(EvaluatedCondition<PodStatus> condition) {
            // unused
        }
    }

    /**
     * Gets node port service address.
     *
     * @param namespace the namespace
     * @param serviceName the service name
     * @return the node port service address
     */
    public static String getNodePortServiceAddress(String namespace, String serviceName) {
        var nodes = kubeClient().getClient().nodes().list().getItems();
        var nodeAddresses = nodes.stream().findFirst()
                .map(Node::getStatus)
                .map(NodeStatus::getAddresses)
                .stream().findFirst()
                .orElseThrow(() -> new KubeClusterException("Unable to get IP of the first node from " + nodes));
        var nodeIP = nodeAddresses.stream().map(NodeAddress::getAddress).findFirst()
                .orElseThrow(() -> new KubeClusterException("Unable to get address of the first node address from " + nodeAddresses));
        var spec = kubeClient().getService(namespace, serviceName).getSpec();
        int port = spec.getPorts().stream().map(ServicePort::getNodePort).findFirst()
                .orElseThrow(() -> new KubeClusterException("Unable to get the service port of " + serviceName));
        String address = nodeIP + ":" + port;
        LOGGER.debug("Deduced nodeport address for service: {} as: {}", serviceName, address);
        return address;
    }
}
