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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.certmanager.api.model.v1.CertificateBuilder;
import io.fabric8.certmanager.api.model.v1.IssuerBuilder;

import io.kroxylicious.systemtests.installation.kroxylicious.CertManager;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

import static io.kroxylicious.systemtests.Constants.WEBHOOK_CONFIG_NAME;
import static io.kroxylicious.systemtests.Constants.WEBHOOK_DEPLOYMENT_NAME;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * Installer for the Kroxylicious admission webhook from distribution manifests.
 */
public class AdmissionWebhook {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdmissionWebhook.class);
    private static final String DISTRIBUTION_DIR = "target/kroxylicious-admission-dist";
    private static final String INSTALL_DIR = DISTRIBUTION_DIR + "/install";
    private static final String ISSUER_NAME = "kroxylicious-webhook-selfsigned";
    private static final String CERT_NAME = "kroxylicious-webhook-cert";

    private final String deploymentNamespace;
    private boolean deleteWebhook = true;

    /**
     * Creates a new AdmissionWebhook installer.
     *
     * @param deploymentNamespace the namespace to deploy the webhook into
     */
    public AdmissionWebhook(String deploymentNamespace) {
        this.deploymentNamespace = deploymentNamespace;
    }

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

        LOGGER.info("Deploying admission webhook in {} namespace", deploymentNamespace);

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

        LOGGER.info("Deleting admission webhook in {} namespace", deploymentNamespace);

        deleteInstallManifests();
        deleteCrd();

        LOGGER.info("Admission webhook deletion complete");
    }

    private boolean isDeployed() {
        return kubeClient().getClient().apps().deployments()
                .inNamespace(deploymentNamespace)
                .withName(WEBHOOK_DEPLOYMENT_NAME)
                .get() != null;
    }

    private void validateDistribution() {
        Path installPath = Path.of(INSTALL_DIR);
        if (!Files.exists(installPath) || !Files.isDirectory(installPath)) {
            throw new IllegalStateException(
                    "Distribution directory not found at " + installPath.toAbsolutePath() +
                            ". Please build the distribution first with: " +
                            "mvn clean install -DskipTests -pl kroxylicious-kubernetes/kroxylicious-admission-dist -am");
        }
    }

    private void applyCrd() {
        LOGGER.info("Applying CRD");
        Path crdPath = Path.of(INSTALL_DIR, "00.CustomResourceDefinition.kroxylicioussidecarconfig.yaml");
        applyManifest(crdPath);
    }

    private void applyInstallManifests() {
        LOGGER.info("Applying install manifests from {}", INSTALL_DIR);
        try (var files = Files.list(Path.of(INSTALL_DIR))) {
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
                .withName(ISSUER_NAME)
                .withNamespace(deploymentNamespace)
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
                .withName(CERT_NAME)
                .withNamespace(deploymentNamespace)
                .addToLabels("app.kubernetes.io/name", "kroxylicious")
                .addToLabels("app.kubernetes.io/component", "webhook")
                .endMetadata()
                .withNewSpec()
                .withSecretName(CERT_NAME)
                .withNewIssuerRef()
                .withName(ISSUER_NAME)
                .withKind("Issuer")
                .endIssuerRef()
                .withCommonName(WEBHOOK_DEPLOYMENT_NAME + "." + deploymentNamespace + ".svc.cluster.local")
                .withDnsNames(
                        WEBHOOK_DEPLOYMENT_NAME,
                        WEBHOOK_DEPLOYMENT_NAME + "." + deploymentNamespace,
                        WEBHOOK_DEPLOYMENT_NAME + "." + deploymentNamespace + ".svc",
                        WEBHOOK_DEPLOYMENT_NAME + "." + deploymentNamespace + ".svc.cluster.local")
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
                .inNamespace(deploymentNamespace)
                .withName(CERT_NAME)
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
        LOGGER.info("Waiting for webhook deployment to become ready");
        DeploymentUtils.waitForDeploymentReady(deploymentNamespace, WEBHOOK_DEPLOYMENT_NAME);

        waitForCaBundleInjected();
    }

    private void waitForCaBundleInjected() {
        LOGGER.info("Waiting for CA bundle to be injected into MutatingWebhookConfiguration");
        kubeClient().getClient().admissionRegistration().v1()
                .mutatingWebhookConfigurations()
                .withName(WEBHOOK_CONFIG_NAME)
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
        try (var files = Files.list(Path.of(INSTALL_DIR))) {
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
        Path crdPath = Path.of(INSTALL_DIR, "00.CustomResourceDefinition.kroxylicioussidecarconfig.yaml");
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
