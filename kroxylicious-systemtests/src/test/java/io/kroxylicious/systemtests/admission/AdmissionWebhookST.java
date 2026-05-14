/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.admission;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;

import io.kroxylicious.kubernetes.api.admission.common.Condition;
import io.kroxylicious.sidecar.v1alpha1.KroxyliciousSidecarConfig;
import io.kroxylicious.sidecar.v1alpha1.KroxyliciousSidecarConfigBuilder;
import io.kroxylicious.systemtests.AbstractSystemTests;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.installation.admission.AdmissionWebhook;
import io.kroxylicious.systemtests.installation.kroxylicious.CertManager;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

import static io.kroxylicious.systemtests.Constants.ADMISSION_DEPLOYMENT_NAME;
import static io.kroxylicious.systemtests.Constants.ADMISSION_NAMESPACE;
import static io.kroxylicious.systemtests.Constants.ADMISSION_REGISTRATION_NAME;
import static io.kroxylicious.systemtests.Constants.KAFKA_DEFAULT_NAMESPACE;
import static io.kroxylicious.systemtests.TestTags.ADMISSION_WEBHOOK;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * System test for the Kroxylicious admission webhook.
 * <p>
 * Phase 1: Validates that the admission webhook can be deployed from distribution
 * manifests and becomes ready. Future phases will test actual sidecar injection.
 */
@Tag(ADMISSION_WEBHOOK)
class AdmissionWebhookST extends AbstractSystemTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdmissionWebhookST.class);
    public static final String KAFKA_CLUSTER_NAME = "my-cluster";
    public static final String NAMESPACE_SIDECAR_INJECTION_LABEL_KEY = "sidecar.kroxylicious.io/injection";

    private static CertManager certManager;
    private static AdmissionWebhook admissionWebhook;

    @BeforeAll
    void setupWebhook() {
        LOGGER.info("Setting up admission webhook system test");

        LOGGER.info("Deploying cert-manager");
        certManager = new CertManager();
        certManager.deploy();

        LOGGER.info("Deploying admission webhook from distribution");
        admissionWebhook = new AdmissionWebhook();
        admissionWebhook.deploy(certManager);

        LOGGER.info("Admission webhook setup complete");
    }

    @Test
    void shouldDeployWebhookSuccessfully() {
        LOGGER.info("Verifying webhook deployment is ready");

        var deployment = kubeClient().getClient().apps().deployments()
                .inNamespace(ADMISSION_NAMESPACE)
                .withName(ADMISSION_DEPLOYMENT_NAME)
                .get();

        assertThat(deployment)
                .withFailMessage("Webhook deployment should exist")
                .isNotNull();

        assertThat(deployment.getSpec().getReplicas())
                .withFailMessage("Webhook should have 2 replicas configured")
                .isEqualTo(2);

        assertThat(deployment.getStatus().getReadyReplicas())
                .withFailMessage("Webhook should have 2 ready replicas")
                .isEqualTo(2);

        LOGGER.info("Verifying MutatingWebhookConfiguration exists");

        var webhookConfig = kubeClient().getClient().admissionRegistration().v1()
                .mutatingWebhookConfigurations()
                .withName(ADMISSION_REGISTRATION_NAME)
                .get();

        assertThat(webhookConfig)
                .withFailMessage("MutatingWebhookConfiguration should exist")
                .isNotNull();

        assertThat(webhookConfig.getWebhooks())
                .withFailMessage("MutatingWebhookConfiguration should have exactly one webhook")
                .hasSize(1);

        LOGGER.info("Verifying CA bundle was injected by cert-manager");

        var caBundle = webhookConfig.getWebhooks().get(0).getClientConfig().getCaBundle();

        assertThat(caBundle)
                .withFailMessage("CA bundle should be injected by cert-manager")
                .isNotNull()
                .isNotBlank();

        LOGGER.info("Webhook deployment verified successfully");
    }

    @Test
    void shouldInjectSidecarAndTransformMessages(String namespace) {
        LOGGER.atInfo().addKeyValue("namespace", namespace).log("Starting sidecar injection test");
        createTargetKafkaClusterIfRequired(namespace);
        enableSidecarInjectionInNamespace(namespace);
        // wait for status and extract from Kafka CR?
        String upstreamBootstrap = "%s-kafka-bootstrap.%s.svc:9092".formatted(KAFKA_CLUSTER_NAME, KAFKA_DEFAULT_NAMESPACE);
        createUppercasingKroxyliciousSidecarConfig(namespace, upstreamBootstrap);

        LOGGER.atInfo().addKeyValue("namespace", namespace)
                .addKeyValue("topicName", topicName)
                .log("Creating Kafka topic");
        KafkaSteps.createTopic(namespace, topicName, upstreamBootstrap, 1, 1);

        createPeriodicLowercaseProducerDeployment(namespace);
        verifyProducerPodHasSidecarInjectedAndEnvironmentConfigured(namespace);
        verifyMessagesStoredInTargetClusterHaveBeenTransformedToUppercase(namespace, upstreamBootstrap);
        LOGGER.atInfo().addKeyValue("namespace", namespace).log("Sidecar injection test completed successfully");
    }

    private void verifyMessagesStoredInTargetClusterHaveBeenTransformedToUppercase(String namespace, String upstreamBootstrap) {
        LOGGER.atInfo()
                .addKeyValue("namespace", namespace)
                .addKeyValue("topicName", topicName)
                .log("Consuming messages directly from upstream Kafka");
        List<ConsumerRecord> records = KroxyliciousSteps.consumeMessages(
                namespace,
                topicName,
                upstreamBootstrap,
                1,
                Duration.ofMinutes(2));

        assertThat(records)
                .withFailMessage("Expected uppercase messages from UpperCasing filter")
                .isNotEmpty()
                .extracting(ConsumerRecord::getPayload)
                .allSatisfy(payload -> {
                    assertThat(payload).isEqualTo("HELLO WORLD");
                });
    }

    private static void verifyProducerPodHasSidecarInjectedAndEnvironmentConfigured(String namespace) {
        LOGGER.atInfo().addKeyValue("namespace", namespace).log("Verifying sidecar was injected into the Deployment's pod");
        Pod pod = kubeClient().getClient().pods()
                .inNamespace(namespace)
                .withLabel("app", "message-producer")
                .list().getItems().get(0);

        assertThat(pod.getSpec().getContainers())
                .withFailMessage("Expected pod to have producer container")
                .extracting(Container::getName)
                .contains("producer");

        assertThat(pod.getSpec().getInitContainers())
                .withFailMessage("Expected pod to have sidecar injected as init container")
                .isNotNull()
                .extracting(Container::getName)
                .contains("kroxylicious-proxy");

        LOGGER.atInfo().addKeyValue("namespace", namespace).log("Verifying KAFKA_BOOTSTRAP_SERVERS env var was set");
        Container appContainer = pod.getSpec().getContainers().stream()
                .filter(c -> "producer".equals(c.getName()))
                .findFirst()
                .orElseThrow();

        assertThat(appContainer.getEnv())
                .filteredOn(env -> "KAFKA_BOOTSTRAP_SERVERS".equals(env.getName()))
                .singleElement()
                .extracting(EnvVar::getValue)
                .asString()
                .isEqualTo("localhost:9092");
    }

    private void createPeriodicLowercaseProducerDeployment(String namespace) {
        LOGGER.atInfo().addKeyValue("namespace", namespace).log("Creating producer Deployment");
        var producerDeployment = new DeploymentBuilder()
                .withNewMetadata()
                .withName("message-producer")
                .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                .withReplicas(1)
                .withNewSelector()
                .addToMatchLabels("app", "message-producer")
                .endSelector()
                .withNewTemplate()
                .withNewMetadata()
                .addToLabels("app", "message-producer")
                .endMetadata()
                .withNewSpec()
                .addNewContainer()
                .withName("producer")
                .withImage("apache/kafka:4.2.0")
                .withCommand("/bin/bash", "-c")
                .withArgs(
                        "while true; do " +
                                "echo 'hello world' | " +
                                "/opt/kafka/bin/kafka-console-producer.sh " +
                                "--bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} " +
                                "--topic " + topicName + "; " +
                                "sleep 2; " +
                                "done")
                .endContainer()
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();

        kubeClient().getClient().resource(producerDeployment).create();

        LOGGER.atInfo().addKeyValue("namespace", namespace).log("Waiting for Deployment to become ready");
        DeploymentUtils.waitForDeploymentReady(namespace, "message-producer");
    }

    private static void createUppercasingKroxyliciousSidecarConfig(String namespace, String upstreamBootstrap) {
        LOGGER.atInfo()
                .addKeyValue("namespace", namespace)
                .log("Creating KroxyliciousSidecarConfig with UpperCasing filter");

        var uppercaseConfig = Map.of(
                "transformation", "UpperCasing",
                "transformationConfig", Map.of("charset", "UTF-8"));

        var sidecarConfig = new KroxyliciousSidecarConfigBuilder()
                .withNewMetadata()
                .withName("uppercase-config")
                .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                .addNewVirtualCluster()
                .withName(KAFKA_CLUSTER_NAME)
                .withTargetBootstrapServers(upstreamBootstrap)
                .endVirtualCluster()
                .addNewFilterDefinition()
                .withName("produce-uppercase")
                .withType("ProduceRequestTransformation")
                .withNewConfig()
                .withAdditionalProperties(uppercaseConfig)
                .endConfig()
                .endFilterDefinition()
                .endSpec()
                .build();

        kubeClient().getClient().resource(sidecarConfig).create();

        LOGGER.atInfo().addKeyValue("namespace", namespace).log("Waiting for KroxyliciousSidecarConfig to be Ready");
        kubeClient().getClient().resources(KroxyliciousSidecarConfig.class)
                .inNamespace(namespace)
                .withName("uppercase-config")
                .waitUntilCondition(
                        ksc -> ksc != null && ksc.getStatus() != null
                                && ksc.getStatus().getConditions() != null
                                && ksc.getStatus().getConditions().stream()
                                        .anyMatch(c -> Condition.Type.Ready.equals(c.getType())
                                                && Condition.Status.TRUE.equals(c.getStatus())),
                        30, TimeUnit.SECONDS);
    }

    private static void enableSidecarInjectionInNamespace(String namespace) {
        LOGGER.atInfo()
                .addKeyValue("namespace", namespace)
                .log("Labeling namespace to enable sidecar injection");

        kubeClient().getClient().namespaces().withName(namespace).edit(ns -> new NamespaceBuilder(ns)
                .editOrNewMetadata()
                .addToLabels(NAMESPACE_SIDECAR_INJECTION_LABEL_KEY, "enabled")
                .endMetadata()
                .build());
    }

    private void createTargetKafkaClusterIfRequired(String namespace) {
        LOGGER.atInfo().addKeyValue("namespace", namespace).log("Checking if Kafka cluster exists in kafka namespace");
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(KAFKA_DEFAULT_NAMESPACE, KAFKA_CLUSTER_NAME);
        if (kafkaPods.isEmpty()) {
            LOGGER.atInfo().addKeyValue("namespace", namespace).log("Deploying Kafka cluster");
            resourceManager.createResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.poolWithDualRoleAndPersistentStorage(KAFKA_DEFAULT_NAMESPACE, KAFKA_CLUSTER_NAME, 1),
                    KafkaTemplates.defaultKafka(KAFKA_DEFAULT_NAMESPACE, KAFKA_CLUSTER_NAME, 1));
        }
        else {
            LOGGER.atInfo().addKeyValue("namespace", namespace).log("Kafka cluster already exists, reusing");
        }
    }

    @AfterAll
    void cleanupWebhook() {
        if (!Environment.SKIP_TEARDOWN) {
            if (admissionWebhook != null) {
                LOGGER.info("Deleting admission webhook");
                admissionWebhook.delete();
            }
            if (certManager != null) {
                LOGGER.info("Deleting cert-manager");
                certManager.delete();
            }
        }
        else {
            LOGGER.info("Skipping webhook cleanup due to SKIP_TEARDOWN");
        }
    }
}
