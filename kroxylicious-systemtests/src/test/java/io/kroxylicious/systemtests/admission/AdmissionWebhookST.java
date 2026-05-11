/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.admission;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.AbstractSystemTests;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.installation.admission.AdmissionWebhook;
import io.kroxylicious.systemtests.installation.kroxylicious.CertManager;

import static io.kroxylicious.systemtests.Constants.ADMISSION_DEPLOYMENT_NAME;
import static io.kroxylicious.systemtests.Constants.ADMISSION_NAMESPACE;
import static io.kroxylicious.systemtests.Constants.ADMISSION_REGISTRATION_NAME;
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
