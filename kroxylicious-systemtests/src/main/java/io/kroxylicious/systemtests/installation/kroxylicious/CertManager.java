/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

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
     */
    public CertManager() {
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

    /**
     * Creates a CA Certificate (isCA=true) signed by the self-signed issuer.
     * cert-manager creates a Secret named {@code caName} containing the CA cert and key.
     *
     * @param namespace the namespace
     * @param caName name used for both the Certificate CR and its backing Secret
     * @return the certificate builder
     */
    public CertificateBuilder caCertificate(String namespace, String caName) {
        //@formatter:off
        return certificate(namespace, caName, caName, SELF_SINGED_ISSUER_NAME, List.of(), "cert sign", "crl sign")
                .editSpec()
                    .withIsCA(true)
                .endSpec();
        //@formatter:on
    }

    /**
     * Creates a CA Issuer backed by the named Secret (produced by {@link #caCertificate}).
     *
     * @param namespace the namespace
     * @param caSecretName the Secret name created by {@link #caCertificate}
     * @return the issuer builder
     */
    public IssuerBuilder caIssuer(String namespace, String caSecretName) {
        //@formatter:off
        return new IssuerBuilder()
                .withNewMetadata()
                    .withName(caSecretName + "-issuer")
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withNewCa()
                        .withSecretName(caSecretName)
                    .endCa()
                .endSpec();
        //@formatter:on
    }

    /**
     * Creates a server Certificate (usages: server auth) signed by {@code SELF_SINGED_ISSUER_NAME}.
     *
     * @param namespace the namespace
     * @param commonName the CN
     * @param dnsNames additional SAN DNS names (the CN is prepended if absent)
     * @return the certificate builder
     */
    public CertificateBuilder certFor(String namespace, String commonName, String... dnsNames) {
        return certForWithIssuer(namespace, SELF_SINGED_ISSUER_NAME, commonName, dnsNames);
    }

    /**
     * Creates a server Certificate (usages: server auth) signed by the named Issuer.
     * The resulting Secret is named "server-certificate".
     *
     * @param namespace the namespace
     * @param issuerName the Issuer that signs this cert
     * @param commonName the CN
     * @param dnsNames additional SAN DNS names (the CN is prepended if absent)
     * @return the certificate builder
     */
    public CertificateBuilder certForWithIssuer(String namespace, String issuerName, String commonName, String... dnsNames) {
        List<String> dnsAliases = new ArrayList<>(List.of(dnsNames));
        if (!dnsAliases.contains(commonName)) {
            dnsAliases.add(0, commonName);
        }
        return certificate(namespace, "server-certificate", commonName, issuerName, dnsAliases, "server auth");
    }

    /**
     * Creates a client Certificate (usages: client auth) signed by the named CA Issuer.
     * The resulting Secret is named {@code commonName + "-cert"}.
     *
     * @param namespace the namespace
     * @param caIssuerName the Issuer that signs this cert (created by {@link #caIssuer})
     * @param commonName the CN, also used as the Secret name prefix
     * @return the certificate builder
     */
    public CertificateBuilder clientCertFor(String namespace, String caIssuerName, String commonName) {
        return certificate(namespace, commonName + "-cert", commonName, caIssuerName, List.of(), "client auth");
    }

    private CertificateBuilder certificate(String namespace, String name, String commonName,
                                           String issuerName, List<String> dnsNames, String... usages) {
        //@formatter:off
        return new CertificateBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withCommonName(commonName)
                    .withSecretName(name)
                    .withNewPrivateKey()
                        .withAlgorithm("RSA")
                        .withEncoding("PKCS8")
                        .withSize(4096)
                    .endPrivateKey()
                    .withDnsNames(dnsNames)
                    .withUsages(usages)
                    .withNewIssuerRef()
                        .withName(issuerName)
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
