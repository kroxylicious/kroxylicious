/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.certmanager.api.model.v1.CertificateBuilder;
import io.fabric8.certmanager.api.model.v1.IssuerBuilder;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.NamespaceUtils;
import io.kroxylicious.systemtests.utils.TestUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Cert manager.
 */
public class CertManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CertManager.class);
    public static final String SELF_SINGED_ISSUER_NAME = "self-signed-issuer";

    public static final String CERT_MANAGER_SERVICE_NAME = "cert-manager";
    public static final String CERT_MANAGER_HELM_REPOSITORY_URL = "https://charts.jetstack.io";
    public static final String CERT_MANAGER_HELM_REPOSITORY_NAME = "jetstack";
    public static final String CERT_MANAGER_HELM_CHART_NAME = "jetstack/cert-manager";

    private boolean deleteCertManager = true;
    private final String deploymentNamespace;

    /**
     * Instantiates a new Cert manager.
     *
     * @throws IOException the io exception
     */
    public CertManager() throws IOException {
        deploymentNamespace = Constants.CERT_MANAGER_NAMESPACE;
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
        if (isDeployed()) {
            LOGGER.warn("Skipping cert manager deployment. It is already deployed!");
            deleteCertManager = false;
            return;
        }

        LOGGER.info("Deploy cert manager in {} namespace", Constants.CERT_MANAGER_NAMESPACE);
        NamespaceUtils.createNamespaceAndPrepare(deploymentNamespace);
        ResourceManager.helmClient().addRepository(CERT_MANAGER_HELM_REPOSITORY_NAME, CERT_MANAGER_HELM_REPOSITORY_URL);
        ResourceManager.helmClient().namespace(deploymentNamespace).install(CERT_MANAGER_HELM_CHART_NAME, CERT_MANAGER_SERVICE_NAME,
                Optional.empty(),
                Optional.of(Path.of(TestUtils.getResourcesURI("helm_cert_manager_overrides.yaml"))),
                Optional.of(Map.of("crds.enabled", "true",
                        "crds.keep", "false")));
    }

    /**
     * Delete cert manager.
     */
    public void delete() {
        if (!deleteCertManager) {
            LOGGER.warn("Skipping cert manager deletion. It was previously installed");
            return;
        }
        LOGGER.info("Deleting Cert Manager in {} namespace", Constants.CERT_MANAGER_NAMESPACE);
        ResourceManager.helmClient().delete(deploymentNamespace, CERT_MANAGER_SERVICE_NAME);
        NamespaceUtils.deleteNamespaceWithWaitAndRemoveFromSet(Constants.CERT_MANAGER_NAMESPACE, ResourceManager.getTestContext().getRequiredTestClass().getName());
    }
}
