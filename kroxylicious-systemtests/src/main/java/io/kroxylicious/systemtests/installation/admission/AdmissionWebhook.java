/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.admission;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.certmanager.api.model.v1.CertificateBuilder;
import io.fabric8.certmanager.api.model.v1.IssuerBuilder;

import io.kroxylicious.systemtests.installation.kroxylicious.CertManager;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

import static io.kroxylicious.systemtests.Constants.ADMISSION_DEPLOYMENT_NAME;
import static io.kroxylicious.systemtests.Constants.ADMISSION_NAMESPACE;
import static io.kroxylicious.systemtests.Constants.ADMISSION_REGISTRATION_NAME;
import static io.kroxylicious.systemtests.Constants.ADMISSION_SERVICE_NAME;
import static io.kroxylicious.systemtests.Constants.ADMISSION_TLS_CERT_NAME;
import static io.kroxylicious.systemtests.Constants.ADMISSION_TLS_ISSUER_NAME;
import static io.kroxylicious.systemtests.Environment.KROXYLICIOUS_ADMISSION_WEBHOOK_INSTALL_DIR;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * Installer for the Kroxylicious admission webhook from distribution manifests.
 */
public class AdmissionWebhook {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdmissionWebhook.class);

    private boolean deleteWebhook = true;

    /**
     * Deploys the admission webhook from distribution manifests.
     *
     * @param certManager the cert-manager instance to use for certificate creation
     */
    public void deploy(CertManager certManager) {
        if (isDeployed()) {
            LOGGER.warn("Skipping admission webhook deployment. It is already deployed!");
            deleteWebhook = false;
            return;
        }

        LOGGER.info("Deploying admission webhook in {} namespace", ADMISSION_NAMESPACE);

        validateDistribution();
        applyCrd();
        applyInstallManifests();
        createCertificateResources(certManager);
        waitForWebhookReady();

        LOGGER.info("Admission webhook deployment complete");
    }

    /**
     * Deletes the admission webhook and associated resources.
     */
    public void delete() {
        if (!deleteWebhook) {
            LOGGER.warn("Skipping admission webhook deletion. It was previously installed");
            return;
        }

        LOGGER.info("Deleting admission webhook in {} namespace", ADMISSION_NAMESPACE);

        deleteInstallManifests();
        deleteCrd();

        LOGGER.info("Admission webhook deletion complete");
    }

    private boolean isDeployed() {
        return kubeClient().getClient().apps().deployments()
                .inNamespace(ADMISSION_NAMESPACE)
                .withName(ADMISSION_DEPLOYMENT_NAME)
                .get() != null;
    }

    private void validateDistribution() {
        Path installPath = Path.of(KROXYLICIOUS_ADMISSION_WEBHOOK_INSTALL_DIR);
        if (!Files.exists(installPath) || !Files.isDirectory(installPath)) {
            throw new IllegalStateException(
                    "Distribution directory not found at " + installPath.toAbsolutePath() +
                            ". Please build the distribution first with: " +
                            "mvn clean install -DskipTests -pl kroxylicious-kubernetes/kroxylicious-admission-dist -am");
        }
    }

    private void applyCrd() {
        LOGGER.info("Applying CRD");
        Path crdPath = Path.of(KROXYLICIOUS_ADMISSION_WEBHOOK_INSTALL_DIR, "00.CustomResourceDefinition.kroxylicioussidecarconfig.yaml");
        applyManifest(crdPath);
    }

    private void applyInstallManifests() {
        LOGGER.info("Applying install manifests from {}", KROXYLICIOUS_ADMISSION_WEBHOOK_INSTALL_DIR);
        try (var files = Files.list(Path.of(KROXYLICIOUS_ADMISSION_WEBHOOK_INSTALL_DIR))) {
            files.filter(p -> {
                Path fileName = p.getFileName();
                return fileName != null && !fileName.toString().startsWith("00.CustomResourceDefinition");
            })
                    .sorted()
                    .forEach(this::applyManifest);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to list install manifests", e);
        }
    }

    private void applyManifest(Path manifestPath) {
        LOGGER.info("Applying manifest: {}", manifestPath.getFileName());
        try (InputStream is = Files.newInputStream(manifestPath)) {
            kubeClient().getClient().load(is).serverSideApply();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to apply manifest: " + manifestPath, e);
        }
    }

    private void createCertificateResources(CertManager certManager) {
        LOGGER.info("Creating cert-manager Issuer and Certificate for webhook TLS");

        var issuer = new IssuerBuilder()
                .withNewMetadata()
                .withName(ADMISSION_TLS_ISSUER_NAME)
                .withNamespace(ADMISSION_NAMESPACE)
                .addToLabels("app.kubernetes.io/name", "kroxylicious")
                .addToLabels("app.kubernetes.io/component", "webhook")
                .endMetadata()
                .withNewSpec()
                .withNewSelfSigned()
                .endSelfSigned()
                .endSpec()
                .build();

        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                new IssuerBuilder(issuer));

        var certificate = new CertificateBuilder()
                .withNewMetadata()
                .withName(ADMISSION_TLS_CERT_NAME)
                .withNamespace(ADMISSION_NAMESPACE)
                .addToLabels("app.kubernetes.io/name", "kroxylicious")
                .addToLabels("app.kubernetes.io/component", "webhook")
                .endMetadata()
                .withNewSpec()
                .withSecretName(ADMISSION_TLS_CERT_NAME)
                .withNewIssuerRef()
                .withName(ADMISSION_TLS_ISSUER_NAME)
                .withKind("Issuer")
                .endIssuerRef()
                .withCommonName(ADMISSION_DEPLOYMENT_NAME + "." + ADMISSION_NAMESPACE + ".svc.cluster.local")
                .withDnsNames(
                        ADMISSION_DEPLOYMENT_NAME,
                        ADMISSION_DEPLOYMENT_NAME + "." + ADMISSION_NAMESPACE,
                        ADMISSION_DEPLOYMENT_NAME + "." + ADMISSION_NAMESPACE + ".svc",
                        ADMISSION_DEPLOYMENT_NAME + "." + ADMISSION_NAMESPACE + ".svc.cluster.local")
                .withNewPrivateKey()
                .withEncoding("PKCS8")
                .endPrivateKey()
                .endSpec()
                .build();

        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                new CertificateBuilder(certificate));

        waitForCertificateReady();
    }

    private void waitForCertificateReady() {
        LOGGER.info("Waiting for Certificate to be ready");
        kubeClient().getClient().resources(io.fabric8.certmanager.api.model.v1.Certificate.class)
                .inNamespace(ADMISSION_NAMESPACE)
                .withName(ADMISSION_TLS_CERT_NAME)
                .waitUntilCondition(
                        cert -> cert != null
                                && cert.getStatus() != null
                                && cert.getStatus().getConditions() != null
                                && cert.getStatus().getConditions().stream()
                                        .anyMatch(c -> "Ready".equals(c.getType())
                                                && "True".equals(c.getStatus())),
                        120, TimeUnit.SECONDS);
    }

    private void waitForWebhookReady() {
        LOGGER.info("Waiting for admission webhook deployment to become ready");
        DeploymentUtils.waitForDeploymentReady(ADMISSION_NAMESPACE, ADMISSION_DEPLOYMENT_NAME);
        LOGGER.info("Waiting for admission webhook service to become ready");
        DeploymentUtils.waitForServiceReady(ADMISSION_NAMESPACE, ADMISSION_SERVICE_NAME, Duration.of(1, ChronoUnit.MINUTES));
        waitForCaBundleInjected();
    }

    private void waitForCaBundleInjected() {
        LOGGER.info("Waiting for CA bundle to be injected into MutatingWebhookConfiguration");
        kubeClient().getClient().admissionRegistration().v1()
                .mutatingWebhookConfigurations()
                .withName(ADMISSION_REGISTRATION_NAME)
                .waitUntilCondition(
                        mwc -> mwc != null
                                && !mwc.getWebhooks().isEmpty()
                                && mwc.getWebhooks().get(0).getClientConfig() != null
                                && mwc.getWebhooks().get(0).getClientConfig().getCaBundle() != null
                                && !mwc.getWebhooks().get(0).getClientConfig().getCaBundle().isBlank(),
                        120, TimeUnit.SECONDS);
    }

    private void deleteInstallManifests() {
        LOGGER.info("Deleting install manifests");
        try (var files = Files.list(Path.of(KROXYLICIOUS_ADMISSION_WEBHOOK_INSTALL_DIR))) {
            files.filter(p -> {
                Path fileName = p.getFileName();
                return fileName != null && !fileName.toString().startsWith("00.CustomResourceDefinition");
            })
                    .sorted()
                    .forEach(this::deleteManifest);
        }
        catch (IOException e) {
            LOGGER.atWarn()
                    .addKeyValue("error", e.getMessage())
                    .log("Failed to list install manifests during deletion");
        }
    }

    private void deleteCrd() {
        LOGGER.info("Deleting CRD");
        Path crdPath = Path.of(KROXYLICIOUS_ADMISSION_WEBHOOK_INSTALL_DIR, "00.CustomResourceDefinition.kroxylicioussidecarconfig.yaml");
        deleteManifest(crdPath);
    }

    private void deleteManifest(Path manifestPath) {
        try (InputStream is = Files.newInputStream(manifestPath)) {
            kubeClient().getClient().load(is).delete();
        }
        catch (Exception e) {
            LOGGER.atWarn()
                    .addKeyValue("manifest", manifestPath.getFileName())
                    .addKeyValue("error", e.getMessage())
                    .log("Failed to delete manifest");
        }
    }
}
