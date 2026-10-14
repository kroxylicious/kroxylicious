/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousBuilder;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyTemplates;

import static io.kroxylicious.systemtests.TestTags.OPERATOR;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * System tests covering {@code KafkaProxy.spec.infrastructure.serviceAccountName}, which lets the
 * proxy pods run as a dedicated, user-managed ServiceAccount rather than the namespace default.
 */
@Tag(OPERATOR)
class OperatorServiceAccountST extends AbstractSystemTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorServiceAccountST.class);
    private static final String PREFIX = "optr-sa";
    private static final Duration ASSERTION_DURATION = Duration.ofSeconds(90);
    private static final String CUSTOM_SERVICE_ACCOUNT_NAME = PREFIX + "-proxy";
    private static final String DEFAULT_SERVICE_ACCOUNT_NAME = "default";
    private static final String RUNNING_PHASE = "Running";
    private final String kafkaClusterName = PREFIX + "-cluster";
    private KroxyliciousOperator kroxyliciousOperator;

    @BeforeAll
    void setupBefore() {
        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }

    @AfterAll
    void cleanUp() {
        if (kroxyliciousOperator != null) {
            kroxyliciousOperator.delete();
        }
    }

    @Test
    void shouldRunProxyPodsUnderConfiguredServiceAccount(String namespace) {
        // Given
        // The ServiceAccount is user-managed, so it must exist before the proxy is deployed, otherwise the rollout stalls.
        createServiceAccount(namespace, CUSTOM_SERVICE_ACCOUNT_NAME);

        // When
        deployProxyWithServiceAccount(namespace, kafkaClusterName, CUSTOM_SERVICE_ACCOUNT_NAME);
        LOGGER.info("Kroxylicious deployed with ServiceAccount: {}", CUSTOM_SERVICE_ACCOUNT_NAME);

        // Then
        assertProxyPodServiceAccount(namespace, CUSTOM_SERVICE_ACCOUNT_NAME);
    }

    @Test
    void shouldRevertToDefaultServiceAccountWhenConfigurationRemoved(String namespace) {
        // Given
        createServiceAccount(namespace, CUSTOM_SERVICE_ACCOUNT_NAME);
        deployProxyWithServiceAccount(namespace, kafkaClusterName, CUSTOM_SERVICE_ACCOUNT_NAME);
        assertProxyPodServiceAccount(namespace, CUSTOM_SERVICE_ACCOUNT_NAME);

        KafkaProxy kafkaProxy = kubeClient(namespace).getClient().resources(KafkaProxy.class).inNamespace(namespace)
                .withName(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME).get();

        // When
        resourceManager.replaceResourceWithRetries(kafkaProxy, current -> current.getSpec().setInfrastructure(null));
        LOGGER.info("ServiceAccount removed from Kafka proxy");

        // Then
        assertProxyPodServiceAccount(namespace, DEFAULT_SERVICE_ACCOUNT_NAME);
    }

    private static void createServiceAccount(String namespace, String serviceAccountName) {
        kubeClient(namespace).getClient()
                .resource(new ServiceAccountBuilder()
                        .withNewMetadata()
                        .withName(serviceAccountName)
                        .withNamespace(namespace)
                        .endMetadata()
                        .build())
                .create();
        LOGGER.info("ServiceAccount {}/{} created", namespace, serviceAccountName);
    }

    /**
     * Deploys a proxy carrying the given ServiceAccount. The base builder also supplies the
     * KafkaProxyIngress, KafkaService and VirtualKafkaCluster the proxy needs in order to start;
     * the KafkaService is left dangling because these tests never exercise the data plane.
     */
    private static void deployProxyWithServiceAccount(String namespace, String clusterName, String serviceAccountName) {
        // @formatter:off
        KafkaProxy kafkaProxy = KroxyliciousKafkaProxyTemplates.defaultKafkaProxyCR(1)
                .editSpec()
                    .withNewInfrastructure()
                        .withServiceAccountName(serviceAccountName)
                    .endInfrastructure()
                .endSpec()
                .build();
        // @formatter:on
        KroxyliciousBuilder.singleNodeBaseBuilder(namespace, clusterName, 1)
                .withKafkaProxy(kafkaProxy)
                .build()
                .createOrUpdateResources();
    }

    private static void assertProxyPodServiceAccount(String namespace, String expectedServiceAccountName) {
        // A pod's ServiceAccount is immutable, so a change is only ever observable on replacement pods. Waiting for the
        // single surviving proxy pod to report the expected account therefore also proves the rollout has converged.
        await().atMost(ASSERTION_DURATION).untilAsserted(() -> assertThat(proxyPods(namespace))
                .as("proxy pods running under ServiceAccount '%s'", expectedServiceAccountName)
                .singleElement()
                .satisfies(pod -> {
                    assertThat(pod.getStatus()).extracting(PodStatus::getPhase).isEqualTo(RUNNING_PHASE);
                    assertThat(pod.getSpec()).extracting(PodSpec::getServiceAccountName).isEqualTo(expectedServiceAccountName);
                }));
    }

    private static List<Pod> proxyPods(String namespace) {
        return kubeClient(namespace).listPods(namespace, "app.kubernetes.io/name", "kroxylicious").stream()
                .filter(pod -> "proxy".equals(pod.getMetadata().getLabels().get("app.kubernetes.io/component")))
                .filter(pod -> pod.getMetadata().getDeletionTimestamp() == null)
                .toList();
    }
}
