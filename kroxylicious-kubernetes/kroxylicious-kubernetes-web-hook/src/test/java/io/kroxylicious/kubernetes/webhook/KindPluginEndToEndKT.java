/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.test.ShellUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test for 3rd party plugin support via OCI image volumes.
 * <p>
 * Creates a Kind cluster with the {@code ImageVolume} feature gate enabled (K8s 1.33+),
 * deploys Strimzi + a single-node Kafka cluster, installs the webhook, and verifies
 * that a plugin filter loaded from an OCI image volume transforms Kafka traffic.
 * <p>
 * The test proves the full lifecycle: plugin image mounted into the sidecar, proxy
 * discovers the filter via ServiceLoader, the filter upper-cases produce request
 * values, and a direct consumer observes the transformation.
 */
@EnabledIf("io.kroxylicious.kubernetes.webhook.KindPluginEndToEndKT#isEnvironmentValid")
class KindPluginEndToEndKT {

    // TODO switch away from using the exec kubectl pattern, and adopt the fabric8 client for driving this test, like with the AbstractWebhookInstallKT

    // TODO switch away from using the exec openssl pattern and use the kroxylicious-certificate-test-support module.

    // TODO factor out an AbstractPluginEndToEndKT, so we can easily write a version that uses minikube instead of kind

    // TODO The the scenario where the kubernetes does not have the OCI volume mounting feature gate enabled.

    private static final Logger LOGGER = LoggerFactory.getLogger(KindPluginEndToEndKT.class);
    private static final Predicate<Stream<String>> ALWAYS_VALID = lines -> true;

    private static final String KIND_CLUSTER_NAME = "kroxy-plugin-test";
    private static final String KIND_CONTEXT = "kind-" + KIND_CLUSTER_NAME;
    private static final String INSTALL_DIR = "target/packaged/install";
    private static final String WEBHOOK_NS = "kroxylicious-webhook";
    private static final String KAFKA_NS = "kafka";
    private static final String TEST_NS = "webhook-plugin-test";
    private static final String TOPIC = "plugin-test-topic";

    private static final WebhookInfo INFO = WebhookInfo.fromResource();

    @BeforeAll
    static void createKindCluster() {
        LOGGER.info("Creating Kind cluster '{}' with ImageVolume feature gate", KIND_CLUSTER_NAME);
        ShellUtils.exec("bash", "-c", """
                cat <<'EOF' | kind create cluster --name %s --config=-
                kind: Cluster
                apiVersion: kind.x-k8s.io/v1alpha4
                featureGates:
                  ImageVolume: true
                nodes:
                  - role: control-plane
                EOF""".formatted(KIND_CLUSTER_NAME));

        LOGGER.info("Loading images into Kind cluster");
        ShellUtils.exec("kind", "load", "image-archive", INFO.imageArchive(),
                "--name", KIND_CLUSTER_NAME);
        ShellUtils.exec("kind", "load", "image-archive", INFO.proxyImageArchive(),
                "--name", KIND_CLUSTER_NAME);
        ShellUtils.exec("kind", "load", "image-archive", INFO.testPluginImageArchive(),
                "--name", KIND_CLUSTER_NAME);
    }

    @AfterAll
    static void deleteKindCluster() {
        LOGGER.info("Deleting Kind cluster '{}'", KIND_CLUSTER_NAME);
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "kind", "delete", "cluster", "--name", KIND_CLUSTER_NAME);
    }

    @Test
    void pluginFilterTransformsTrafficThroughSidecar() {
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
        ShellUtils.exec("kubectl", "create", "namespace", KAFKA_NS,
                "--context", KIND_CONTEXT);
        ShellUtils.exec("kubectl", "create",
                "-f", "https://strimzi.io/install/latest?namespace=" + KAFKA_NS,
                "-n", KAFKA_NS, "--context", KIND_CONTEXT);

        LOGGER.info("Waiting for Strimzi operator to become ready");
        ShellUtils.exec("kubectl", "wait", "-n", KAFKA_NS,
                "--for=condition=Available", "--timeout=280s",
                "deployment/strimzi-cluster-operator",
                "--context", KIND_CONTEXT);

        LOGGER.info("Creating single-node Kafka cluster");
        ShellUtils.exec("bash", "-c", """
                cat <<'EOF' | kubectl apply -n %s --context %s -f -
                apiVersion: kafka.strimzi.io/v1beta2
                kind: KafkaNodePool
                metadata:
                  name: dual-role
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
                apiVersion: kafka.strimzi.io/v1beta2
                kind: Kafka
                metadata:
                  name: my-cluster
                  annotations:
                    strimzi.io/kraft: enabled
                    strimzi.io/node-pools: enabled
                spec:
                  kafka:
                    replicas: 1
                    listeners:
                      - name: plain
                        port: 9092
                        type: internal
                        tls: false
                    config:
                      offsets.topic.replication.factor: 1
                      transaction.state.log.replication.factor: 1
                      transaction.state.log.min.isr: 1
                    storage:
                      type: ephemeral
                  entityOperator:
                    topicOperator: {}
                EOF""".formatted(KAFKA_NS, KIND_CONTEXT));

        LOGGER.info("Waiting for Kafka cluster to become ready");
        ShellUtils.exec("kubectl", "wait", "kafka/my-cluster",
                "-n", KAFKA_NS, "--for=condition=Ready", "--timeout=280s",
                "--context", KIND_CONTEXT);
    }

    // --- Webhook installation (mirrors AbstractWebhookInstallKT) ---

    private void installWebhook() {
        LOGGER.info("Installing CRDs");
        ShellUtils.exec("kubectl", "apply", "-f",
                "../kroxylicious-kubernetes-api/src/main/resources/META-INF/fabric8/kroxylicioussidecarconfigs.kroxylicious.io-v1.yml",
                "--context", KIND_CONTEXT);

        LOGGER.info("Applying namespace and RBAC manifests (00-02)");
        for (int i = 0; i <= 2; i++) {
            applyManifestsWithPrefix(String.format("%02d.", i));
        }

        LOGGER.info("Creating self-signed TLS certificate for webhook");
        createWebhookTlsSecret();

        LOGGER.info("Applying deployment and webhook manifests (03-05)");
        for (int i = 3; i <= 5; i++) {
            applyManifestsWithPrefix(String.format("%02d.", i));
        }

        LOGGER.info("Patching MutatingWebhookConfiguration with CA bundle");
        ShellUtils.exec("bash", "-c", """
                kubectl patch mutatingwebhookconfiguration kroxylicious-sidecar-injector \
                --context %s \
                --type=json -p='[{"op":"add","path":"/webhooks/0/clientConfig/caBundle",\
                "value":"'$(base64 -w0 target/webhook-tls.crt)'"}]'""".formatted(KIND_CONTEXT));

        LOGGER.info("Waiting for webhook deployment to become ready");
        ShellUtils.exec("kubectl", "wait", "-n", WEBHOOK_NS,
                "--for=jsonpath={.status.readyReplicas}=1",
                "--timeout=280s", "deployment", "kroxylicious-webhook",
                "--context", KIND_CONTEXT);
        LOGGER.info("Webhook deployment is ready");
    }

    private void applyManifestsWithPrefix(String prefix) {
        Path installDir = Path.of(INSTALL_DIR);
        try (var files = java.nio.file.Files.list(installDir)) {
            files.filter(p -> p.getFileName().toString().startsWith(prefix))
                    .sorted()
                    .forEach(p -> {
                        LOGGER.info("Applying {}", p.getFileName());
                        ShellUtils.exec("kubectl", "apply", "-f", p.toString(),
                                "--context", KIND_CONTEXT);
                    });
        }
        catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
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
        ShellUtils.exec("kubectl", "create", "secret", "tls", "kroxylicious-webhook-cert",
                "-n", WEBHOOK_NS,
                "--cert=target/webhook-tls.crt",
                "--key=target/webhook-tls.key",
                "--context", KIND_CONTEXT);
    }

    // --- Test namespace with plugin config ---

    private void createTestNamespaceAndConfig() {
        LOGGER.info("Creating test namespace with injection label");
        ShellUtils.exec("kubectl", "create", "namespace", TEST_NS,
                "--context", KIND_CONTEXT);
        ShellUtils.exec("kubectl", "label", "namespace", TEST_NS,
                "kroxylicious.io/sidecar-injection=enabled",
                "--context", KIND_CONTEXT);

        LOGGER.info("Creating KroxyliciousSidecarConfig with plugin and filter");
        ShellUtils.exec("bash", "-c", """
                cat <<'EOF' | kubectl apply -n %s --context %s -f -
                apiVersion: kroxylicious.io/v1alpha1
                kind: KroxyliciousSidecarConfig
                metadata:
                  name: test-config
                spec:
                  upstreamBootstrapServers: my-cluster-kafka-bootstrap.%s.svc.cluster.local:9092
                  plugins:
                    - name: simple-transform
                      image:
                        reference: %s
                        pullPolicy: Never
                  filterDefinitions:
                    - name: uppercase-produce
                      type: ProduceRequestTransformation
                      config:
                        transformation: UpperCasing
                        transformationConfig:
                          charset: UTF-8
                EOF""".formatted(TEST_NS, KIND_CONTEXT, KAFKA_NS,
                INFO.testPluginImageName()));

        LOGGER.info("Waiting for webhook to observe sidecar config");
        try {
            Thread.sleep(5000);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // --- Producer + verification ---

    private void runProducerAndVerify() {
        String kafkaImage = discoverKafkaImage();
        LOGGER.atInfo()
                .addKeyValue("kafkaImage", kafkaImage)
                .log("Creating producer pod with sidecar injection");
        // The producer sends 3 lowercase messages via kafka-console-producer.
        // The sidecar's ProduceRequestTransformation + UpperCasing filter
        // transforms them before they reach Kafka.
        ShellUtils.exec("bash", "-c", """
                cat <<'EOF' | kubectl apply -n %s --context %s -f -
                apiVersion: v1
                kind: Pod
                metadata:
                  name: test-producer
                spec:
                  restartPolicy: Never
                  terminationGracePeriodSeconds: 0
                  containers:
                    - name: producer
                      image: %s
                      command:
                        - /bin/sh
                        - -c
                        - >-
                          sleep 15 &&
                          printf 'hello-from-sidecar\\nhello-from-sidecar\\nhello-from-sidecar\\n' |
                          /opt/kafka/bin/kafka-console-producer.sh
                          --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS
                          --topic %s
                EOF""".formatted(TEST_NS, KIND_CONTEXT, kafkaImage, TOPIC));

        // TODO The sleep 15 above is inelegant. The pod should just run indefinitely and
        // this test code should delete the pod at the end of the test.

        verifyPodStructure();
        waitForProxyReady();
        verifyProducerCompletes();
        verifyConsumedMessagesUpperCased();
    }

    private void verifyPodStructure() {
        LOGGER.info("Waiting for producer pod to be created");
        ShellUtils.exec("kubectl", "wait", "pod/test-producer",
                "-n", TEST_NS, "--for=condition=PodScheduled", "--timeout=60s",
                "--context", KIND_CONTEXT);

        LOGGER.info("Verifying sidecar was injected");
        assertThat(ShellUtils.execValidate(
                lines -> lines.anyMatch(line -> line.contains("kroxylicious-proxy")),
                ALWAYS_VALID,
                "kubectl", "get", "pod", "test-producer", "-n", TEST_NS,
                "--context", KIND_CONTEXT,
                "-o", "jsonpath={.spec.containers[*].name}{.spec.initContainers[*].name}"))
                .as("Pod should have kroxylicious-proxy sidecar container")
                .isTrue();

        LOGGER.info("Verifying plugin volume was mounted");
        assertThat(ShellUtils.execValidate(
                lines -> lines.anyMatch(line -> line.contains("plugin-simple-transform")),
                ALWAYS_VALID,
                "kubectl", "get", "pod", "test-producer", "-n", TEST_NS,
                "--context", KIND_CONTEXT,
                "-o", "jsonpath={.spec.volumes[*].name}"))
                .as("Pod should have plugin-simple-transform volume")
                .isTrue();

        LOGGER.info("Verifying OCI image volume (not emptyDir)");
        assertThat(ShellUtils.execValidate(
                lines -> lines.anyMatch(line -> line.contains(INFO.testPluginImageName())),
                ALWAYS_VALID,
                "kubectl", "get", "pod", "test-producer", "-n", TEST_NS,
                "--context", KIND_CONTEXT,
                "-o", "jsonpath={.spec.volumes[?(@.name=='plugin-simple-transform')].image.reference}"))
                .as("Plugin volume should be an OCI image volume")
                .isTrue();
    }

    private void waitForProxyReady() {
        // The proxy is a native sidecar (initContainer with restartPolicy: Always),
        // so its status appears in initContainerStatuses, not containerStatuses.
        LOGGER.info("Waiting for sidecar proxy to become ready");
        try {
            ShellUtils.exec("kubectl", "wait", "pod/test-producer",
                    "-n", TEST_NS,
                    "--for=jsonpath={.status.initContainerStatuses[?(@.name=='kroxylicious-proxy')].ready}=true",
                    "--timeout=120s",
                    "--context", KIND_CONTEXT);
        }
        catch (AssertionError e) {
            dumpPodDiagnostics();
            throw new AssertionError("Sidecar proxy did not become ready", e);
        }
        LOGGER.info("Sidecar proxy is ready");
    }

    private void dumpPodDiagnostics() {
        LOGGER.error("Proxy did not become ready, dumping diagnostics");
        Predicate<Stream<String>> logDescribe = lines -> {
            lines.forEach(line -> LOGGER.error("[describe] {}", line));
            return true;
        };
        ShellUtils.execValidate(logDescribe, ALWAYS_VALID,
                "kubectl", "describe", "pod/test-producer", "-n", TEST_NS,
                "--context", KIND_CONTEXT);
        Predicate<Stream<String>> logProxyLogs = lines -> {
            lines.forEach(line -> LOGGER.error("[proxy-logs] {}", line));
            return true;
        };
        ShellUtils.execValidate(logProxyLogs, ALWAYS_VALID,
                "kubectl", "logs", "test-producer", "-c", "kroxylicious-proxy",
                "-n", TEST_NS, "--context", KIND_CONTEXT);
    }

    /**
     * Discovers the Kafka container image from the Strimzi-managed Kafka pod,
     * so we use the same image (already cached on the Kind node) for producer/consumer pods.
     */
    private String discoverKafkaImage() {
        var imageRef = new AtomicReference<String>();
        ShellUtils.execValidate(
                lines -> {
                    String image = lines.findFirst().orElse("").trim();
                    imageRef.set(image);
                    return !image.isBlank();
                },
                ALWAYS_VALID,
                "kubectl", "get", "pod", "-n", KAFKA_NS,
                "-l", "strimzi.io/cluster=my-cluster",
                "-l", "strimzi.io/kind=Kafka",
                "-o", "jsonpath={.items[0].spec.containers[0].image}",
                "--context", KIND_CONTEXT);
        return imageRef.get();
    }

    private void verifyProducerCompletes() {
        LOGGER.info("Waiting for producer to complete");
        // Wait for the producer container to finish (it sends 3 records then exits)
        ShellUtils.exec("kubectl", "wait", "pod/test-producer",
                "-n", TEST_NS,
                "--for=jsonpath={.status.containerStatuses[?(@.name=='producer')].state.terminated.reason}=Completed",
                "--timeout=120s",
                "--context", KIND_CONTEXT);
        LOGGER.info("Producer completed");
    }

    private void verifyConsumedMessagesUpperCased() {
        String kafkaImage = discoverKafkaImage();
        LOGGER.info("Running consumer directly against Kafka to verify transformation");
        // Run a consumer pod in the kafka namespace (no injection) to read back the messages
        ShellUtils.exec("bash", "-c", """
                cat <<'EOF' | kubectl apply -n %s --context %s -f -
                apiVersion: v1
                kind: Pod
                metadata:
                  name: test-consumer
                spec:
                  restartPolicy: Never
                  terminationGracePeriodSeconds: 0
                  containers:
                    - name: consumer
                      image: %s
                      command:
                        - /bin/sh
                        - -c
                        - >-
                          /opt/kafka/bin/kafka-console-consumer.sh
                          --bootstrap-server my-cluster-kafka-bootstrap.%s.svc.cluster.local:9092
                          --topic %s
                          --from-beginning
                          --max-messages 3
                          --timeout-ms 60000
                EOF""".formatted(KAFKA_NS, KIND_CONTEXT, kafkaImage, KAFKA_NS, TOPIC));

        LOGGER.info("Waiting for consumer to complete");
        ShellUtils.exec("kubectl", "wait", "pod/test-consumer",
                "-n", KAFKA_NS,
                "--for=jsonpath={.status.containerStatuses[?(@.name=='consumer')].state.terminated.reason}=Completed",
                "--timeout=120s",
                "--context", KIND_CONTEXT);

        LOGGER.info("Checking consumer output for upper-cased messages");
        assertThat(ShellUtils.execValidate(
                lines -> lines.anyMatch(line -> line.contains("HELLO-FROM-SIDECAR")),
                ALWAYS_VALID,
                "kubectl", "logs", "test-consumer", "-n", KAFKA_NS,
                "--context", KIND_CONTEXT))
                .as("Consumer should see upper-cased message values, proving the plugin filter ran")
                .isTrue();

        LOGGER.info("Plugin end-to-end test passed: messages were transformed by the filter");
    }

    // --- Cleanup ---

    private void cleanup() {
        LOGGER.info("Cleaning up test resources");
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "kubectl", "delete", "namespace", TEST_NS, "--ignore-not-found",
                "--context", KIND_CONTEXT);
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "kubectl", "delete", "pod", "test-consumer", "-n", KAFKA_NS,
                "--ignore-not-found", "--context", KIND_CONTEXT);
        // Delete webhook
        for (int i = 5; i >= 0; i--) {
            deleteManifestsWithPrefix(String.format("%02d.", i));
        }
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "kubectl", "delete", "-f",
                "../kroxylicious-kubernetes-api/src/main/resources/META-INF/fabric8/kroxylicioussidecarconfigs.kroxylicious.io-v1.yml",
                "--ignore-not-found", "--context", KIND_CONTEXT);
        // Delete Kafka + Strimzi
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "kubectl", "delete", "namespace", KAFKA_NS, "--ignore-not-found",
                "--context", KIND_CONTEXT);
    }

    private void deleteManifestsWithPrefix(String prefix) {
        Path installDir = Path.of(INSTALL_DIR);
        try (var files = java.nio.file.Files.list(installDir)) {
            files.filter(p -> p.getFileName().toString().startsWith(prefix))
                    .sorted()
                    .forEach(p -> ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                            "kubectl", "delete", "-f", p.toString(),
                            "--ignore-not-found", "--context", KIND_CONTEXT));
        }
        catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    // --- Environment validation ---

    public static boolean isEnvironmentValid() {
        return ShellUtils.validateToolsOnPath("kind", "kubectl", "openssl")
                && allImageArchivesExist();
    }

    private static boolean allImageArchivesExist() {
        return exists(INFO.imageArchive(), "webhook")
                && exists(INFO.proxyImageArchive(), "proxy")
                && exists(INFO.testPluginImageArchive(), "test-plugin");
    }

    private static boolean exists(String path, String label) {
        boolean present = Path.of(path).toFile().exists();
        if (!present) {
            LOGGER.warn("{} image archive not found: {}", label, path);
        }
        return present;
    }

    // TODO have a test that covers indirectly createds pods, e.g. pods which are part of the deployment, sts, job.
}
