/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;

import io.kroxylicious.kubernetes.api.admission.common.Condition;
import io.kroxylicious.sidecar.v1alpha1.KroxyliciousSidecarConfig;
import io.kroxylicious.sidecar.v1alpha1.KroxyliciousSidecarConfigBuilder;
import io.kroxylicious.testing.integration.ShellUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Tests that the webhook can be installed from YAML manifests and that sidecar
 * injection works. Abstract because it does not define how a cluster is provided
 * or how images are loaded — subclasses handle that.
 */
abstract class AbstractWebhookInstallKT {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWebhookInstallKT.class);
    static final Predicate<Stream<String>> ALWAYS_VALID = lines -> true;

    private static final String INSTALL_DIR = "target/packaged/install";
    private static final String WEBHOOK_NS = "kroxylicious-webhook";
    private static final String TEST_NS = "webhook-test";

    private static final Path CRD_PATH = Path.of(
            "../kroxylicious-admission-api/src/main/resources/META-INF/fabric8/kroxylicioussidecarconfigs.sidecar.kroxylicious.io-v1.yml");

    private KubernetesClient client;

    static boolean testImageAvailable() {
        String imageArchive = WebhookInfo.fromResource().imageArchive();
        assumeThat(Path.of(imageArchive))
                .describedAs("Container image archive %s must exist", imageArchive)
                .withFailMessage("Container image archive %s did not exist", imageArchive)
                .exists();
        return true;
    }

    @Test
    void shouldInstallAndInjectSidecar() {
        client = new KubernetesClientBuilder().build();
        try {
            installWebhook();
            waitForWebhookReady();
            createTestNamespaceAndConfig();
            verifyInjection();
            verifySecretMount();
            verifyOptOut();
            verifyFailClosed();
        }
        finally {
            cleanup();
        }
    }

    private void installWebhook() {
        LOGGER.info("Installing CRDs");
        applyCrds();

        LOGGER.info("Applying install manifests");
        applyAllManifests();

        LOGGER.info("Creating self-signed TLS certificate for webhook");
        createWebhookTlsSecret();

        LOGGER.info("Patching MutatingWebhookConfiguration with CA bundle");
        patchWebhookCaBundle();
    }

    private void applyCrds() {
        try (InputStream is = Files.newInputStream(CRD_PATH)) {
            client.load(is).serverSideApply();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    /**
     * Generates a self-signed certificate and creates the TLS Secret that the
     * webhook deployment expects.
     */
    private void createWebhookTlsSecret() {
        // Generate self-signed cert with openssl
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
                    .addToStringData("tls.crt", Files.readString(Path.of("target/webhook-tls.crt")))
                    .addToStringData("tls.key", Files.readString(Path.of("target/webhook-tls.key")))
                    .build();
            client.resource(secret).create();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Patches the MutatingWebhookConfiguration with the CA bundle so the API
     * server trusts the webhook. Must be called after the webhook configuration
     * manifest (05) has been applied.
     */
    private void patchWebhookCaBundle() {
        try {
            byte[] certBytes = Files.readAllBytes(Path.of("target/webhook-tls.crt"));
            String caBundle = Base64.getEncoder().encodeToString(certBytes);
            client.admissionRegistration().v1()
                    .mutatingWebhookConfigurations()
                    .withName("kroxylicious-sidecar-injector")
                    .edit(mwc -> {
                        mwc.getWebhooks().get(0).getClientConfig().setCaBundle(caBundle);
                        return mwc;
                    });
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void waitForWebhookReady() {
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

    private void createTestNamespaceAndConfig() {
        LOGGER.info("Creating test namespace with injection label");
        var ns = new NamespaceBuilder()
                .withNewMetadata()
                .withName(TEST_NS)
                .addToLabels("sidecar.kroxylicious.io/injection", "enabled")
                .endMetadata()
                .build();
        client.namespaces().resource(ns).create();

        LOGGER.info("Creating test Secret for secret mount verification");
        var testSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("kms-credentials")
                .withNamespace(TEST_NS)
                .endMetadata()
                .addToStringData("credentials.json", "{\"key\": \"test-value\"}")
                .build();
        client.resource(testSecret).create();

        LOGGER.info("Creating KroxyliciousSidecarConfig");
        var sidecarConfig = new KroxyliciousSidecarConfigBuilder()
                .withNewMetadata()
                .withName("test-config")
                .withNamespace(TEST_NS)
                .endMetadata()
                .withNewSpec()
                .addNewVirtualCluster()
                .withName("sidecar")
                .withTargetBootstrapServers("kafka-bootstrap.kafka.svc.cluster.local:9092")
                .endVirtualCluster()
                .addNewSecretMount()
                .withName("kms")
                .withSecretName("kms-credentials")
                .endSecretMount()
                .endSpec()
                .build();
        client.resource(sidecarConfig).create();

        LOGGER.info("Waiting for webhook to set Ready condition on sidecar config");
        client.resources(KroxyliciousSidecarConfig.class)
                .inNamespace(TEST_NS)
                .withName("test-config")
                .waitUntilCondition(
                        ksc -> ksc != null
                                && ksc.getStatus() != null
                                && ksc.getStatus().getConditions() != null
                                && ksc.getStatus().getConditions().stream()
                                        .anyMatch(c -> Condition.Type.Ready.equals(c.getType())
                                                && Condition.Status.TRUE.equals(c.getStatus())),
                        30, TimeUnit.SECONDS);
    }

    private void verifyInjection() {
        LOGGER.info("Creating test pod to verify sidecar injection");
        var testPod = new PodBuilder()
                .withNewMetadata()
                .withName("test-app")
                .withNamespace(TEST_NS)
                .endMetadata()
                .withNewSpec()
                .withTerminationGracePeriodSeconds(0L)
                .addNewContainer()
                .withName("app")
                .withImage("busybox:latest")
                .withCommand("sleep", "3600")
                .endContainer()
                .endSpec()
                .build();
        Pod created = client.resource(testPod).create();

        // Verify sidecar container was injected
        LOGGER.info("Verifying sidecar container was injected");
        var allContainerNames = new ArrayList<String>();
        created.getSpec().getContainers().forEach(c -> allContainerNames.add(c.getName()));
        if (created.getSpec().getInitContainers() != null) {
            created.getSpec().getInitContainers().forEach(c -> allContainerNames.add(c.getName()));
        }
        assertThat(allContainerNames)
                .as("Pod should have kroxylicious-proxy sidecar container")
                .contains("kroxylicious-proxy");

        // Verify the proxy config annotation was set
        assertThat(created.getMetadata().getAnnotations())
                .as("Pod should have config-generation annotation")
                .containsKey("sidecar.kroxylicious.io/config-generation");

        // Verify KAFKA_BOOTSTRAP_SERVERS env var was set on the app container
        Container appContainer = created.getSpec().getContainers().stream()
                .filter(c -> "app".equals(c.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("app container not found in pod"));
        assertThat(appContainer.getEnv())
                .as("App container should have KAFKA_BOOTSTRAP_SERVERS pointing to localhost")
                .filteredOn(env -> "KAFKA_BOOTSTRAP_SERVERS".equals(env.getName()))
                .singleElement()
                .extracting(EnvVar::getValue)
                .asString()
                .contains("localhost:");

        LOGGER.info("Sidecar injection verified successfully");
    }

    private void verifySecretMount() {
        LOGGER.info("Creating test pod to verify secret mount injection");
        var testPod = new PodBuilder()
                .withNewMetadata()
                .withName("test-app-secret")
                .withNamespace(TEST_NS)
                .endMetadata()
                .withNewSpec()
                .withTerminationGracePeriodSeconds(0L)
                .addNewContainer()
                .withName("app")
                .withImage("busybox:latest")
                .withCommand("sleep", "3600")
                .endContainer()
                .endSpec()
                .build();
        Pod created = client.resource(testPod).create();

        LOGGER.info("Verifying secret volume was added to pod");
        List<Volume> volumes = created.getSpec().getVolumes();
        assertThat(volumes)
                .as("Pod should have a secret-kms volume")
                .filteredOn(v -> "secret-kms".equals(v.getName()))
                .singleElement()
                .satisfies(v -> assertThat(v.getSecret().getSecretName()).isEqualTo("kms-credentials"));

        LOGGER.info("Verifying secret volume mount on sidecar container");
        var allContainers = new ArrayList<Container>();
        allContainers.addAll(created.getSpec().getContainers());
        if (created.getSpec().getInitContainers() != null) {
            allContainers.addAll(created.getSpec().getInitContainers());
        }
        Container sidecar = allContainers.stream()
                .filter(c -> InjectionDecision.SIDECAR_CONTAINER_NAME.equals(c.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("sidecar container not found in pod"));

        assertThat(sidecar.getVolumeMounts())
                .as("Sidecar should have secret-kms volume mount")
                .filteredOn(vm -> "secret-kms".equals(vm.getName()))
                .singleElement()
                .satisfies(vm -> {
                    assertThat(vm.getMountPath()).isEqualTo("/opt/kroxylicious/secrets/kms");
                    assertThat(vm.getReadOnly()).isTrue();
                });

        LOGGER.info("Verifying app container does NOT have the secret volume mount");
        Container appContainer = created.getSpec().getContainers().stream()
                .filter(c -> "app".equals(c.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("app container not found in pod"));
        List<VolumeMount> appMounts = appContainer.getVolumeMounts();
        if (appMounts != null) {
            assertThat(appMounts)
                    .as("App container should NOT have the secret volume mount")
                    .filteredOn(vm -> "secret-kms".equals(vm.getName()))
                    .isEmpty();
        }

        LOGGER.info("Secret mount injection verified successfully");
    }

    private void verifyOptOut() {
        LOGGER.info("Creating opted-out pod to verify opt-out works");
        var optedOutPod = new PodBuilder()
                .withNewMetadata()
                .withName("test-app-no-sidecar")
                .withNamespace(TEST_NS)
                .addToLabels("sidecar.kroxylicious.io/injection", "disabled")
                .endMetadata()
                .withNewSpec()
                .withTerminationGracePeriodSeconds(0L)
                .addNewContainer()
                .withName("app")
                .withImage("busybox:latest")
                .withCommand("sleep", "3600")
                .endContainer()
                .endSpec()
                .build();
        Pod created = client.resource(optedOutPod).create();

        // Verify sidecar was NOT injected
        assertThat(created.getSpec().getContainers())
                .as("Pod's containers should not be empty")
                .isNotEmpty()
                .extracting(Container::getName)
                .as("Opted-out pod should not have sidecar")
                .doesNotContain("kroxylicious-proxy");

        LOGGER.info("Opt-out verified successfully");
    }

    private void verifyFailClosed() {
        LOGGER.info("Scaling webhook deployment to 0 replicas to verify fail-closed behaviour");
        client.apps().deployments()
                .inNamespace(WEBHOOK_NS)
                .withName("kroxylicious-webhook")
                .scale(0);

        client.apps().deployments()
                .inNamespace(WEBHOOK_NS)
                .withName("kroxylicious-webhook")
                .waitUntilCondition(
                        d -> d != null
                                && d.getStatus() != null
                                && (d.getStatus().getReadyReplicas() == null
                                        || d.getStatus().getReadyReplicas() == 0),
                        60, TimeUnit.SECONDS);

        // Wait for the Service endpoints to be drained so the API server
        // considers the webhook unreachable
        client.endpoints()
                .inNamespace(WEBHOOK_NS)
                .withName("kroxylicious-webhook")
                .waitUntilCondition(
                        ep -> ep == null
                                || ep.getSubsets() == null
                                || ep.getSubsets().isEmpty()
                                || ep.getSubsets().stream()
                                        .allMatch(s -> s.getAddresses() == null
                                                || s.getAddresses().isEmpty()),
                        60, TimeUnit.SECONDS);

        LOGGER.info("Webhook scaled to 0, attempting pod creation (expecting rejection)");
        var pod = new PodBuilder()
                .withNewMetadata()
                .withName("test-app-fail-closed")
                .withNamespace(TEST_NS)
                .endMetadata()
                .withNewSpec()
                .withTerminationGracePeriodSeconds(0L)
                .addNewContainer()
                .withName("app")
                .withImage("busybox:latest")
                .withCommand("sleep", "3600")
                .endContainer()
                .endSpec()
                .build();

        assertThatThrownBy(() -> client.resource(pod).create())
                .isInstanceOf(KubernetesClientException.class)
                .hasMessageContaining("sidecar-injector.kroxylicious.io");

        LOGGER.info("Fail-closed verified: pod creation rejected when webhook unavailable");

        LOGGER.info("Scaling webhook back to 2 replicas");
        client.apps().deployments()
                .inNamespace(WEBHOOK_NS)
                .withName("kroxylicious-webhook")
                .scale(2);
        waitForWebhookReady();
    }

    private void cleanup() {
        LOGGER.info("Cleaning up test resources");
        if (client == null) {
            return;
        }
        ignoreCleanupErrors("test namespace",
                () -> client.namespaces().withName(TEST_NS).delete());
        deleteAllManifests();
        ignoreCleanupErrors("CRD",
                () -> {
                    try (InputStream is = Files.newInputStream(CRD_PATH)) {
                        client.load(is).delete();
                    }
                });
        ignoreCleanupErrors("Kubernetes client", client::close);
    }

    private void deleteAllManifests() {
        Path installDir = Path.of(INSTALL_DIR);
        try (var files = Files.list(installDir)) {
            files.sorted()
                    .forEach(p -> ignoreCleanupErrors(p.getFileName().toString(),
                            () -> {
                                try (InputStream is = Files.newInputStream(p)) {
                                    client.load(is).delete();
                                }
                            }));
        }
        catch (IOException e) {
            LOGGER.atWarn().setCause(e).log("failed to list install directory during cleanup");
        }
    }

    private static void ignoreCleanupErrors(String description, CleanupAction action) {
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

    static boolean validateKubeContext(String expectedContext) {
        try {
            Config config = Config.autoConfigure(null);
            var context = config.getCurrentContext();
            return context != null && expectedContext.equals(context.getName());
        }
        catch (Exception e) {
            return false;
        }
    }
}
