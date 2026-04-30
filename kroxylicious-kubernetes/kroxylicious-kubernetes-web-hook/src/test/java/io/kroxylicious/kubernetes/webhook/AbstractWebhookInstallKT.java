/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.nio.file.Path;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.test.ShellUtils;

import static org.assertj.core.api.Assertions.assertThat;
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
        try {
            installWebhook();
            waitForWebhookReady();
            createTestNamespaceAndConfig();
            verifyInjection();
            verifyOptOut();
        }
        finally {
            cleanup();
        }
    }

    private void installWebhook() {
        // Apply everything except the cert-manager Certificate (06.Certificate.*)
        // Instead we generate a self-signed cert and create the Secret directly
        LOGGER.info("Installing CRDs");
        applyCrds();

        // Apply namespace, service account, RBAC (00-02)
        LOGGER.info("Applying namespace and RBAC manifests (00-02)");
        for (int i = 0; i <= 2; i++) {
            String prefix = String.format("%02d.", i);
            applyManifestsWithPrefix(prefix);
        }

        // Create TLS secret before the Deployment tries to mount it
        LOGGER.info("Creating self-signed TLS certificate for webhook");
        createWebhookTlsSecret();

        // Apply deployment, service, webhook config (03-05)
        LOGGER.info("Applying deployment and webhook manifests (03-05)");
        for (int i = 3; i <= 5; i++) {
            String prefix = String.format("%02d.", i);
            applyManifestsWithPrefix(prefix);
        }

        // Patch the MutatingWebhookConfiguration with the CA bundle
        // (normally cert-manager does this via the inject-ca-from annotation)
        LOGGER.info("Patching MutatingWebhookConfiguration with CA bundle");
        patchWebhookCaBundle();
    }

    private void applyCrds() {
        // The KroxyliciousSidecarConfig CRD is in kroxylicious-kubernetes-api
        assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "kubectl", "apply", "-f",
                "../kroxylicious-kubernetes-api/src/main/resources/META-INF/fabric8/kroxylicioussidecarconfigs.kroxylicious.io-v1.yml"))
                .isTrue();
    }

    private void applyManifestsWithPrefix(String prefix) {
        Path installDir = Path.of(INSTALL_DIR);
        try (var files = java.nio.file.Files.list(installDir)) {
            files.filter(p -> p.getFileName().toString().startsWith(prefix))
                    .sorted()
                    .forEach(p -> {
                        LOGGER.info("Applying {}", p.getFileName());
                        assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                                "kubectl", "apply", "-f", p.toString()))
                                .isTrue();
                    });
        }
        catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
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

        // Create the Secret
        ShellUtils.exec(
                "kubectl", "create", "secret", "tls", "kroxylicious-webhook-cert",
                "-n", WEBHOOK_NS,
                "--cert=target/webhook-tls.crt",
                "--key=target/webhook-tls.key");
    }

    /**
     * Patches the MutatingWebhookConfiguration with the CA bundle so the API
     * server trusts the webhook. Must be called after the webhook configuration
     * manifest (05) has been applied.
     */
    private void patchWebhookCaBundle() {
        ShellUtils.exec("bash", "-c", """
                kubectl patch mutatingwebhookconfiguration kroxylicious-sidecar-injector \
                --type=json -p='[{"op":"add","path":"/webhooks/0/clientConfig/caBundle",\
                "value":"'$(base64 -w0 target/webhook-tls.crt)'"}]'""");
    }

    private void waitForWebhookReady() {
        LOGGER.info("Waiting for webhook deployment to become ready");
        assertThat(ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "kubectl", "wait", "-n", WEBHOOK_NS,
                "--for=jsonpath={.status.readyReplicas}=1",
                "--timeout=300s",
                "deployment", "kroxylicious-webhook"))
                .isTrue();
        LOGGER.info("Webhook deployment is ready");
    }

    private void createTestNamespaceAndConfig() {
        LOGGER.info("Creating test namespace with injection label");
        ShellUtils.exec("kubectl", "create", "namespace", TEST_NS);
        ShellUtils.exec("kubectl", "label", "namespace", TEST_NS,
                "kroxylicious.io/sidecar-injection=enabled");

        LOGGER.info("Creating KroxyliciousSidecarConfig");
        ShellUtils.exec("bash", "-c", """
                cat <<'EOF' | kubectl apply -n %s -f -
                apiVersion: kroxylicious.io/v1alpha1
                kind: KroxyliciousSidecarConfig
                metadata:
                  name: test-config
                spec:
                  upstreamBootstrapServers: kafka-bootstrap.kafka.svc.cluster.local:9092
                EOF""".formatted(TEST_NS));

        // Wait for the webhook's informer to sync the config

        LOGGER.info("Waiting for webhook to observe sidecar config");
        try {
            // TODO replace this sleep with watching for a Ready condition in the
            // status of the KroxyliciousSidecarConfig
            Thread.sleep(5000);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void verifyInjection() {
        LOGGER.info("Creating test pod to verify sidecar injection");
        ShellUtils.exec("bash", "-c", """
                cat <<'EOF' | kubectl apply -n %s -f -
                apiVersion: v1
                kind: Pod
                metadata:
                  name: test-app
                spec:
                  terminationGracePeriodSeconds: 0
                  containers:
                    - name: app
                      image: busybox:latest
                      command: ["sleep", "3600"]
                EOF""".formatted(TEST_NS));

        // Verify sidecar container was injected
        LOGGER.info("Verifying sidecar container was injected");
        assertThat(ShellUtils.execValidate(
                lines -> lines.anyMatch(line -> line.contains("kroxylicious-proxy")),
                ALWAYS_VALID,
                "kubectl", "get", "pod", "test-app", "-n", TEST_NS,
                "-o", "jsonpath={.spec.containers[*].name}{.spec.initContainers[*].name}"))
                .as("Pod should have kroxylicious-proxy sidecar container")
                .isTrue();

        // Verify the proxy config annotation was set
        assertThat(ShellUtils.execValidate(
                lines -> lines.anyMatch(line -> line.contains("injected")),
                ALWAYS_VALID,
                "kubectl", "get", "pod", "test-app", "-n", TEST_NS,
                "-o", "jsonpath={.metadata.annotations.kroxylicious\\.io/sidecar-status}"))
                .as("Pod should have sidecar-status annotation set to 'injected'")
                .isTrue();

        // Verify KAFKA_BOOTSTRAP_SERVERS env var was set on the app container
        assertThat(ShellUtils.execValidate(
                lines -> lines.anyMatch(line -> line.contains("localhost:")),
                ALWAYS_VALID,
                "kubectl", "get", "pod", "test-app", "-n", TEST_NS,
                "-o", "jsonpath={.spec.containers[?(@.name=='app')].env[?(@.name=='KAFKA_BOOTSTRAP_SERVERS')].value}"))
                .as("App container should have KAFKA_BOOTSTRAP_SERVERS pointing to localhost")
                .isTrue();

        LOGGER.info("Sidecar injection verified successfully");
    }

    private void verifyOptOut() {
        LOGGER.info("Creating opted-out pod to verify opt-out works");
        ShellUtils.exec("bash", "-c", """
                cat <<'EOF' | kubectl apply -n %s -f -
                apiVersion: v1
                kind: Pod
                metadata:
                  name: test-app-no-sidecar
                  labels:
                    kroxylicious.io/inject-sidecar: "false"
                spec:
                  terminationGracePeriodSeconds: 0
                  containers:
                    - name: app
                      image: busybox:latest
                      command: ["sleep", "3600"]
                EOF""".formatted(TEST_NS));

        // Verify sidecar was NOT injected
        assertThat(ShellUtils.execValidate(
                lines -> lines.noneMatch(line -> line.contains("kroxylicious-proxy")),
                ALWAYS_VALID,
                "kubectl", "get", "pod", "test-app-no-sidecar", "-n", TEST_NS,
                "-o", "jsonpath={.spec.containers[*].name}"))
                .as("Opted-out pod should not have sidecar")
                .isTrue();

        LOGGER.info("Opt-out verified successfully");
    }

    private void cleanup() {
        LOGGER.info("Cleaning up test resources");
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "kubectl", "delete", "namespace", TEST_NS, "--ignore-not-found");
        // Delete install manifests (00-05)
        for (int i = 5; i >= 0; i--) {
            String prefix = String.format("%02d.", i);
            deleteManifestsWithPrefix(prefix);
        }
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                "kubectl", "delete", "-f",
                "../kroxylicious-kubernetes-api/src/main/resources/META-INF/fabric8/kroxylicioussidecarconfigs.kroxylicious.io-v1.yml",
                "--ignore-not-found");
    }

    private void deleteManifestsWithPrefix(String prefix) {
        Path installDir = Path.of(INSTALL_DIR);
        try (var files = java.nio.file.Files.list(installDir)) {
            files.filter(p -> p.getFileName().toString().startsWith(prefix))
                    .sorted()
                    .forEach(p -> ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID,
                            "kubectl", "delete", "-f", p.toString(), "--ignore-not-found"));
        }
        catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    static boolean validateKubeContext(String expectedContext) {
        return ShellUtils.execValidate(
                lines -> lines.anyMatch(line -> line.contains(expectedContext)),
                ALWAYS_VALID,
                "kubectl", "config", "current-context");
    }
}
