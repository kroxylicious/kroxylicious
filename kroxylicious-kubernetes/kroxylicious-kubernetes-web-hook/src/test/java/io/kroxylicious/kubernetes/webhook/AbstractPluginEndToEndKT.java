/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfig;
import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfigBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kroxylicioussidecarconfigspec.plugins.Image;
import io.kroxylicious.test.ShellUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test for 3rd party plugin support via OCI image volumes.
 * Deploys Strimzi + a single-node Kafka cluster, installs the webhook, and verifies
 * that a plugin filter loaded from an OCI image volume transforms Kafka traffic.
 * Subclasses provide cluster lifecycle and image loading.
 */
abstract class AbstractPluginEndToEndKT {

    // TODO switch away from using the exec openssl pattern and use the kroxylicious-certificate-test-support module.
    // TODO The the scenario where the kubernetes does not have the OCI volume mounting feature gate enabled.
    // TODO have a test that covers indirectly creates pods, e.g. pods which are part of the deployment, sts, job.

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPluginEndToEndKT.class);
    static final Predicate<Stream<String>> ALWAYS_VALID = lines -> true;

    static final String INSTALL_DIR = "target/packaged/install";
    static final String WEBHOOK_NS = "kroxylicious-webhook";
    static final String KAFKA_NS = "kafka";
    static final String TEST_NS = "webhook-plugin-test";
    static final String TOPIC = "plugin-test-topic";

    private static final Path CRD_PATH = Path.of(
            "../kroxylicious-kubernetes-api/src/main/resources/META-INF/fabric8/"
                    + "kroxylicioussidecarconfigs.kroxylicious.io-v1.yml");

    static final WebhookInfo INFO = WebhookInfo.fromResource();

    private KubernetesClient client;

    abstract String kubeContext();

    @Test
    void pluginFilterTransformsTrafficThroughSidecar() {
        Config config = Config.autoConfigure(kubeContext());
        client = new KubernetesClientBuilder().withConfig(config).build();
        try {
            installStrimzi();
            installWebhook();
            createTestNamespaceAndConfig();
            runProducerAndVerify();
        }
        finally {
            cleanup();
        }
    }

    // --- Strimzi + Kafka ---

    private void installStrimzi() {
        LOGGER.info("Installing Strimzi operator");
        var kafkaNs = new NamespaceBuilder()
                .withNewMetadata()
                .withName(KAFKA_NS)
                .endMetadata()
                .build();
        client.namespaces().resource(kafkaNs).create();

        try (InputStream is = URI.create(strimziInstallUrl()).toURL().openStream()) {
            client.load(is).serverSideApply();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        LOGGER.info("Waiting for Strimzi CRDs to be established");
        waitForCrdEstablished("kafkas.kafka.strimzi.io");
        waitForCrdEstablished("kafkanodepools.kafka.strimzi.io");

        LOGGER.info("Waiting for Strimzi operator to become ready");
        client.apps().deployments()
                .inNamespace(KAFKA_NS)
                .withName("strimzi-cluster-operator")
                .waitUntilCondition(
                        d -> d != null
                                && d.getStatus() != null
                                && d.getStatus().getReadyReplicas() != null
                                && d.getStatus().getReadyReplicas() >= 1,
                        280, TimeUnit.SECONDS);

        LOGGER.info("Creating single-node Kafka cluster");
        String kafkaYaml = """
                apiVersion: kafka.strimzi.io/v1
                kind: KafkaNodePool
                metadata:
                  name: dual-role
                  namespace: %s
                  labels:
                    strimzi.io/cluster: my-cluster
                spec:
                  replicas: 1
                  roles:
                    - controller
                    - broker
                  storage:
                    type: ephemeral
                ---
                apiVersion: kafka.strimzi.io/v1
                kind: Kafka
                metadata:
                  name: my-cluster
                  namespace: %s
                  annotations:
                    strimzi.io/node-pools: enabled
                spec:
                  kafka:
                    listeners:
                      - name: plain
                        port: 9092
                        type: internal
                        tls: false
                    config:
                      offsets.topic.replication.factor: 1
                      transaction.state.log.replication.factor: 1
                      transaction.state.log.min.isr: 1
                  entityOperator:
                    topicOperator: {}
                """.formatted(KAFKA_NS, KAFKA_NS);
        try (InputStream is = new ByteArrayInputStream(
                kafkaYaml.getBytes(StandardCharsets.UTF_8))) {
            client.load(is).serverSideApply();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        LOGGER.info("Waiting for Kafka cluster to become ready");
        client.genericKubernetesResources("kafka.strimzi.io/v1", "Kafka")
                .inNamespace(KAFKA_NS)
                .withName("my-cluster")
                .waitUntilCondition(
                        kafka -> hasCondition(kafka, "Ready", "True"),
                        280, TimeUnit.SECONDS);
    }

    // --- Webhook installation ---

    private void installWebhook() {
        LOGGER.info("Installing CRDs");
        try (InputStream is = Files.newInputStream(CRD_PATH)) {
            client.load(is).serverSideApply();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        LOGGER.info("Waiting for KSC CRD to be established");
        waitForCrdEstablished("kroxylicioussidecarconfigs.kroxylicious.io");

        LOGGER.info("Applying install manifests");
        applyAllManifests();

        LOGGER.info("Creating self-signed TLS certificate for webhook");
        createWebhookTlsSecret();

        LOGGER.info("Patching MutatingWebhookConfiguration with CA bundle");
        patchWebhookCaBundle();

        LOGGER.info("Waiting for webhook deployment to become ready");
        client.apps().deployments()
                .inNamespace(WEBHOOK_NS)
                .withName("kroxylicious-webhook")
                .waitUntilCondition(
                        d -> d != null
                                && d.getStatus() != null
                                && d.getStatus().getReadyReplicas() != null
                                && d.getStatus().getReadyReplicas() >= 1,
                        300, TimeUnit.SECONDS);
        LOGGER.info("Webhook deployment is ready");
    }

    private void applyAllManifests() {
        Path installDir = Path.of(INSTALL_DIR);
        try (var files = Files.list(installDir)) {
            files.sorted()
                    .forEach(p -> {
                        LOGGER.info("Applying {}", p.getFileName());
                        try (InputStream is = Files.newInputStream(p)) {
                            client.load(is).serverSideApply();
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void createWebhookTlsSecret() {
        ShellUtils.exec(
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", "target/webhook-tls.key",
                "-out", "target/webhook-tls.crt",
                "-days", "1",
                "-nodes",
                "-subj", "/CN=kroxylicious-webhook.kroxylicious-webhook.svc",
                "-addext", """
                        subjectAltName=\
                        DNS:kroxylicious-webhook,\
                        DNS:kroxylicious-webhook.kroxylicious-webhook,\
                        DNS:kroxylicious-webhook.kroxylicious-webhook.svc,\
                        DNS:kroxylicious-webhook.kroxylicious-webhook.svc.cluster.local""");
        try {
            var secret = new SecretBuilder()
                    .withNewMetadata()
                    .withName("kroxylicious-webhook-cert")
                    .withNamespace(WEBHOOK_NS)
                    .endMetadata()
                    .withType("kubernetes.io/tls")
                    .addToStringData("tls.crt",
                            Files.readString(Path.of("target/webhook-tls.crt")))
                    .addToStringData("tls.key",
                            Files.readString(Path.of("target/webhook-tls.key")))
                    .build();
            client.resource(secret).create();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void patchWebhookCaBundle() {
        try {
            byte[] certBytes = Files.readAllBytes(Path.of("target/webhook-tls.crt"));
            String caBundle = Base64.getEncoder().encodeToString(certBytes);
            client.admissionRegistration().v1()
                    .mutatingWebhookConfigurations()
                    .withName("kroxylicious-sidecar-injector")
                    .edit(mwc -> {
                        mwc.getWebhooks().get(0)
                                .getClientConfig()
                                .setCaBundle(caBundle);
                        return mwc;
                    });
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // --- Test namespace with plugin config ---

    private void createTestNamespaceAndConfig() {
        LOGGER.info("Creating test namespace with injection label");
        var ns = new NamespaceBuilder()
                .withNewMetadata()
                .withName(TEST_NS)
                .addToLabels("kroxylicious.io/sidecar-injection", "enabled")
                .endMetadata()
                .build();
        client.namespaces().resource(ns).create();

        LOGGER.info("Creating KroxyliciousSidecarConfig with plugin and filter");
        var ksc = new KroxyliciousSidecarConfigBuilder()
                .withNewMetadata()
                .withName("test-config")
                .withNamespace(TEST_NS)
                .endMetadata()
                .withNewSpec()
                .withTargetBootstrapServers(
                        "my-cluster-kafka-bootstrap." + KAFKA_NS
                                + ".svc.cluster.local:9092")
                .addNewPlugin()
                .withName("simple-transform")
                .withNewImage()
                .withReference(INFO.testPluginImageName())
                .withPullPolicy(Image.PullPolicy.NEVER)
                .endImage()
                .endPlugin()
                .addNewFilterDefinition()
                .withName("uppercase-produce")
                .withType("ProduceRequestTransformation")
                .withConfig(Map.of(
                        "transformation", "UpperCasing",
                        "transformationConfig", Map.of("charset", "UTF-8")))
                .endFilterDefinition()
                .endSpec()
                .build();
        client.resource(ksc).create();

        LOGGER.info("Waiting for webhook to set Ready condition on sidecar config");
        client.resources(KroxyliciousSidecarConfig.class)
                .inNamespace(TEST_NS)
                .withName("test-config")
                .waitUntilCondition(
                        c -> c != null
                                && c.getStatus() != null
                                && c.getStatus().getConditions() != null
                                && c.getStatus().getConditions().stream()
                                        .anyMatch(cond -> Condition.Type.Ready
                                                .equals(cond.getType())
                                                && Condition.Status.TRUE
                                                        .equals(cond.getStatus())),
                        30, TimeUnit.SECONDS);
    }

    // --- Producer + verification ---

    private void runProducerAndVerify() {
        String kafkaImage = discoverKafkaImage();
        LOGGER.atInfo()
                .addKeyValue("kafkaImage", kafkaImage)
                .log("Creating producer pod with sidecar injection");

        var producerPod = new PodBuilder()
                .withNewMetadata()
                .withName("test-producer")
                .withNamespace(TEST_NS)
                .endMetadata()
                .withNewSpec()
                .withRestartPolicy("Never")
                .withTerminationGracePeriodSeconds(0L)
                .addNewContainer()
                .withName("producer")
                .withImage(kafkaImage)
                .withCommand("/bin/sh", "-c",
                        "sleep 15 && "
                                + "printf 'hello-from-sidecar\\n"
                                + "hello-from-sidecar\\n"
                                + "hello-from-sidecar\\n' | "
                                + "/opt/kafka/bin/kafka-console-producer.sh "
                                + "--bootstrap-server "
                                + "$KAFKA_BOOTSTRAP_SERVERS "
                                + "--topic " + TOPIC)
                .endContainer()
                .endSpec()
                .build();

        // TODO The sleep 15 above is inelegant. The pod should just run indefinitely and
        Pod created = client.resource(producerPod).create();

        verifyPodStructure(created);
        waitForProxyReady();
        verifyProducerCompletes();
        verifyConsumedMessagesUpperCased();
    }

    private void verifyPodStructure(Pod pod) {
        LOGGER.info("Verifying sidecar was injected");
        var allContainerNames = new ArrayList<String>();
        pod.getSpec().getContainers()
                .forEach(c -> allContainerNames.add(c.getName()));
        if (pod.getSpec().getInitContainers() != null) {
            pod.getSpec().getInitContainers()
                    .forEach(c -> allContainerNames.add(c.getName()));
        }
        assertThat(allContainerNames)
                .as("Pod should have kroxylicious-proxy sidecar container")
                .contains("kroxylicious-proxy");

        LOGGER.info("Verifying plugin volume was mounted");
        assertThat(pod.getSpec().getVolumes())
                .as("Pod should have plugin-simple-transform volume")
                .extracting(Volume::getName)
                .contains("plugin-simple-transform");

        LOGGER.info("Verifying OCI image volume (not emptyDir)");
        var pluginVolume = pod.getSpec().getVolumes().stream()
                .filter(v -> "plugin-simple-transform".equals(v.getName()))
                .findFirst()
                .orElseThrow();
        assertThat(pluginVolume.getImage())
                .as("Plugin volume should be an OCI image volume")
                .isNotNull();
        assertThat(pluginVolume.getImage().getReference())
                .as("Plugin volume should reference the test plugin image")
                .contains(INFO.testPluginImageName());
    }

    private void waitForProxyReady() {
        LOGGER.info("Waiting for sidecar proxy to become ready");
        try {
            client.pods()
                    .inNamespace(TEST_NS)
                    .withName("test-producer")
                    .waitUntilCondition(
                            pod -> pod != null
                                    && pod.getStatus() != null
                                    && pod.getStatus()
                                            .getInitContainerStatuses() != null
                                    && pod.getStatus()
                                            .getInitContainerStatuses().stream()
                                            .anyMatch(s -> "kroxylicious-proxy"
                                                    .equals(s.getName())
                                                    && Boolean.TRUE
                                                            .equals(s.getReady())),
                            120, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            dumpPodDiagnostics();
            throw new AssertionError("Sidecar proxy did not become ready", e);
        }
        LOGGER.info("Sidecar proxy is ready");
    }

    private void dumpPodDiagnostics() {
        LOGGER.error("Proxy did not become ready, dumping diagnostics");
        try {
            Pod pod = client.pods()
                    .inNamespace(TEST_NS)
                    .withName("test-producer")
                    .get();
            if (pod != null) {
                LOGGER.atError()
                        .addKeyValue("podStatus", pod.getStatus())
                        .log("pod status");
            }
            String proxyLogs = client.pods()
                    .inNamespace(TEST_NS)
                    .withName("test-producer")
                    .inContainer("kroxylicious-proxy")
                    .getLog();
            LOGGER.atError()
                    .addKeyValue("logs", proxyLogs)
                    .log("proxy container logs");
        }
        catch (Exception e) {
            LOGGER.atWarn()
                    .setCause(e)
                    .log("failed to retrieve diagnostics");
        }
    }

    private String discoverKafkaImage() {
        var pods = client.pods()
                .inNamespace(KAFKA_NS)
                .withLabel("strimzi.io/cluster", "my-cluster")
                .withLabel("strimzi.io/kind", "Kafka")
                .list();
        assertThat(pods.getItems())
                .as("Expected at least one Kafka broker pod")
                .isNotEmpty();
        return pods.getItems().get(0)
                .getSpec()
                .getContainers().get(0)
                .getImage();
    }

    private void verifyProducerCompletes() {
        LOGGER.info("Waiting for producer to complete");
        client.pods()
                .inNamespace(TEST_NS)
                .withName("test-producer")
                .waitUntilCondition(
                        pod -> pod != null
                                && pod.getStatus() != null
                                && pod.getStatus()
                                        .getContainerStatuses() != null
                                && pod.getStatus()
                                        .getContainerStatuses().stream()
                                        .anyMatch(s -> "producer"
                                                .equals(s.getName())
                                                && s.getState() != null
                                                && s.getState()
                                                        .getTerminated() != null
                                                && "Completed".equals(
                                                        s.getState()
                                                                .getTerminated()
                                                                .getReason())),
                        120, TimeUnit.SECONDS);
        LOGGER.info("Producer completed");
    }

    private void verifyConsumedMessagesUpperCased() {
        String kafkaImage = discoverKafkaImage();
        LOGGER.info("Running consumer to verify transformation");

        var consumerPod = new PodBuilder()
                .withNewMetadata()
                .withName("test-consumer")
                .withNamespace(KAFKA_NS)
                .endMetadata()
                .withNewSpec()
                .withRestartPolicy("Never")
                .withTerminationGracePeriodSeconds(0L)
                .addNewContainer()
                .withName("consumer")
                .withImage(kafkaImage)
                .withCommand("/bin/sh", "-c",
                        "/opt/kafka/bin/kafka-console-consumer.sh "
                                + "--bootstrap-server my-cluster-kafka-bootstrap."
                                + KAFKA_NS + ".svc.cluster.local:9092 "
                                + "--topic " + TOPIC + " "
                                + "--from-beginning "
                                + "--max-messages 3 "
                                + "--timeout-ms 60000")
                .endContainer()
                .endSpec()
                .build();
        client.resource(consumerPod).create();

        LOGGER.info("Waiting for consumer to complete");
        client.pods()
                .inNamespace(KAFKA_NS)
                .withName("test-consumer")
                .waitUntilCondition(
                        pod -> pod != null
                                && pod.getStatus() != null
                                && pod.getStatus()
                                        .getContainerStatuses() != null
                                && pod.getStatus()
                                        .getContainerStatuses().stream()
                                        .anyMatch(s -> "consumer"
                                                .equals(s.getName())
                                                && s.getState() != null
                                                && s.getState()
                                                        .getTerminated() != null
                                                && "Completed".equals(
                                                        s.getState()
                                                                .getTerminated()
                                                                .getReason())),
                        120, TimeUnit.SECONDS);

        LOGGER.info("Checking consumer output for upper-cased messages");
        String logs = client.pods()
                .inNamespace(KAFKA_NS)
                .withName("test-consumer")
                .inContainer("consumer")
                .getLog();
        assertThat(logs)
                .as("Consumer should see upper-cased messages, "
                        + "proving the plugin filter ran")
                .contains("HELLO-FROM-SIDECAR");

        LOGGER.info("Plugin end-to-end test passed");
    }

    // --- Cleanup ---

    private void cleanup() {
        LOGGER.info("Cleaning up test resources");
        if (client == null) {
            return;
        }
        ignoreCleanupErrors("test namespace",
                () -> client.namespaces().withName(TEST_NS).delete());
        ignoreCleanupErrors("test-consumer pod",
                () -> client.pods()
                        .inNamespace(KAFKA_NS)
                        .withName("test-consumer")
                        .delete());
        deleteAllManifests();
        ignoreCleanupErrors("KSC CRD",
                () -> {
                    try (InputStream is = Files.newInputStream(CRD_PATH)) {
                        client.load(is).delete();
                    }
                });
        ignoreCleanupErrors("Strimzi operator",
                () -> {
                    try (InputStream is = URI.create(strimziInstallUrl())
                            .toURL().openStream()) {
                        client.load(is).delete();
                    }
                });
        ignoreCleanupErrors("kafka namespace",
                () -> client.namespaces().withName(KAFKA_NS).delete());
        ignoreCleanupErrors("Kubernetes client", client::close);
    }

    private void deleteAllManifests() {
        Path installDir = Path.of(INSTALL_DIR);
        try (var files = Files.list(installDir)) {
            files.sorted()
                    .forEach(p -> ignoreCleanupErrors(
                            p.getFileName().toString(),
                            () -> {
                                try (InputStream is = Files.newInputStream(p)) {
                                    client.load(is).delete();
                                }
                            }));
        }
        catch (IOException e) {
            LOGGER.atWarn()
                    .setCause(e)
                    .log("failed to list install directory during cleanup");
        }
    }

    // --- Helpers ---

    private String strimziInstallUrl() {
        return "https://strimzi.io/install/latest?namespace=" + KAFKA_NS;
    }

    private void waitForCrdEstablished(String crdName) {
        client.apiextensions().v1().customResourceDefinitions()
                .withName(crdName)
                .waitUntilCondition(
                        crd -> crd != null
                                && crd.getStatus() != null
                                && crd.getStatus().getConditions() != null
                                && crd.getStatus().getConditions().stream()
                                        .anyMatch(c -> "Established"
                                                .equals(c.getType())
                                                && "True"
                                                        .equals(c.getStatus())),
                        60, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private static boolean hasCondition(
                                        GenericKubernetesResource resource,
                                        String type,
                                        String status) {
        if (resource == null) {
            return false;
        }
        Object statusObj = resource.getAdditionalProperties().get("status");
        if (!(statusObj instanceof Map<?, ?> statusMap)) {
            return false;
        }
        Object conditionsObj = statusMap.get("conditions");
        if (!(conditionsObj instanceof List<?> conditions)) {
            return false;
        }
        return conditions.stream()
                .filter(Map.class::isInstance)
                .map(c -> (Map<String, Object>) c)
                .anyMatch(c -> type.equals(c.get("type"))
                        && status.equals(c.get("status")));
    }

    private static void ignoreCleanupErrors(
                                            String description,
                                            CleanupAction action) {
        try {
            action.run();
        }
        catch (Exception e) {
            LOGGER.atWarn()
                    .addKeyValue("resource", description)
                    .setCause(e)
                    .log("cleanup failed");
        }
    }

    @FunctionalInterface
    interface CleanupAction {
        void run() throws Exception;
    }

    // --- Shared validation helpers ---

    static boolean allImageArchivesExist() {
        return exists(INFO.imageArchive(), "webhook")
                && exists(INFO.proxyImageArchive(), "proxy")
                && exists(INFO.testPluginImageArchive(), "test-plugin");
    }

    static boolean exists(String path, String label) {
        boolean present = Path.of(path).toFile().exists();
        if (!present) {
            LOGGER.atWarn()
                    .addKeyValue("label", label)
                    .addKeyValue("path", path)
                    .log("image archive not found");
        }
        return present;
    }

    static boolean validateKubeContext(String expectedContext) {
        try {
            Config config = Config.autoConfigure(null);
            var context = config.getCurrentContext();
            return context != null
                    && expectedContext.equals(context.getName());
        }
        catch (Exception e) {
            return false;
        }
    }
}
