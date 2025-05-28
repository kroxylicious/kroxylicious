/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.certmanager.api.model.v1.CertificateBuilder;
import io.fabric8.certmanager.api.model.v1.IssuerBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.dsl.NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Cert manager.
 */
public class CertManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CertManager.class);
    public static final String SELF_SINGED_ISSUER_NAME = "self-signed-issuer";

    private final NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata> deployment;
    private boolean deleteCertManager = true;

    /**
     * Instantiates a new Cert manager.
     *
     * @throws IOException the io exception
     */
    public CertManager() throws IOException {
        deployment = kubeClient().getClient()
                .load(DeploymentUtils.getDeploymentFileFromURL(Constants.CERT_MANAGER_URL));
    }

    public IssuerBuilder issuer(String namespace) {
        //@formatter:off
        return new IssuerBuilder()
                .withNewMetadata()
                    .withName(SELF_SINGED_ISSUER_NAME)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withNewSelfSigned()
                .endSelfSigned()
                .endSpec();
        //@formatter:on
    }

    public CertificateBuilder certFor(String namespace, String commonName, String... dnsNames) {
        List<String> dnsAliases = new ArrayList<>(List.of(dnsNames));
        if (!dnsAliases.contains(commonName)) {
            dnsAliases.add(0, commonName);
        }

        //@formatter:off
        return new CertificateBuilder()
                .withNewMetadata()
                    .withName("server-certificate")
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withCommonName(commonName)
                    .withSecretName("server-certificate")
                    .withNewPrivateKey()
                        .withAlgorithm("RSA")
                        .withEncoding("PKCS8")
                        .withSize(4096)
                    .endPrivateKey()
                    .withDnsNames(dnsAliases)
                    .withUsages("server auth")
                    .withNewIssuerRef()
                        .withName(SELF_SINGED_ISSUER_NAME)
                        .withKind("Issuer")
                        .withGroup("cert-manager.io")
                    .endIssuerRef()
                .endSpec();
        //@formatter:on
    }

    private boolean isDeployed() {
        return !kubeClient().getClient().apps().deployments().inAnyNamespace().withLabelSelector("app=cert-manager").list().getItems().isEmpty();
    }

    /**
     * Deploy cert manager.
     */
    public void deploy() {
        LOGGER.info("Deploy cert manager in {} namespace", Constants.CERT_MANAGER_NAMESPACE);
        if (isDeployed()) {
            LOGGER.warn("Skipping cert manager deployment. It is already deployed!");
            deleteCertManager = false;
            return;
        }
        deployment.create();
        DeploymentUtils.waitForDeploymentReady(Constants.CERT_MANAGER_NAMESPACE, "cert-manager-webhook");
    }

    /**
     * Delete cert manager.
     * @throws IOException the io exception
     */
    public void delete() throws IOException {
        if (!deleteCertManager) {
            LOGGER.warn("Skipping cert manager deletion. It was previously installed");
            return;
        }
        LOGGER.info("Deleting Cert Manager in {} namespace", Constants.CERT_MANAGER_NAMESPACE);
        deployment.withGracePeriod(0).delete();
        NamespaceUtils.deleteNamespaceWithWait(Constants.CERT_MANAGER_NAMESPACE);
    }
}
