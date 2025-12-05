/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;

import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.TlsBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.kms.ExperimentalKmsConfig;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousConfigMapTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousFilterTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaClusterRefTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyIngressTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousSecretTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousVirtualKafkaClusterTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaUserTemplates;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Kroxylicious.
 */
public class Kroxylicious {
    private static final Logger LOGGER = LoggerFactory.getLogger(Kroxylicious.class);
    private final String deploymentNamespace;
    private final ResourceManager resourceManager = ResourceManager.getInstance();

    /**
     * Instantiates a new Kroxylicious Service to be used in kubernetes.
     *
     * @param deploymentNamespace the deployment namespace
     */
    public Kroxylicious(String deploymentNamespace) {
        this.deploymentNamespace = deploymentNamespace;
    }

    private void createRecordEncryptionFilterConfigMap(TestKmsFacade<?, ?, ?> testKmsFacade, ExperimentalKmsConfig experimentalKmsConfig) {
        LOGGER.info("Deploy Kroxylicious config Map with record encryption filter in {} namespace", deploymentNamespace);
        resourceManager.createResourceFromBuilderWithWait(
                KroxyliciousFilterTemplates.kroxyliciousRecordEncryptionFilter(deploymentNamespace, testKmsFacade, experimentalKmsConfig));
    }

    private void createAuthorizationFilterConfigMap(String clusterName, Map<String, String> usernamePassword, List<String> aclRules) {
        LOGGER.info("Deploy Kroxylicious with authorization filter in {} namespace", deploymentNamespace);
        String passwordSuffix = "-password";

        usernamePassword.forEach((user, password) -> resourceManager.createResourceFromBuilderWithWait(
                KroxyliciousSecretTemplates.createPasswordSecret(Constants.KAFKA_DEFAULT_NAMESPACE, user + passwordSuffix, password),
                KroxyliciousSecretTemplates.createPasswordSecret(deploymentNamespace, user + passwordSuffix, password),
                KafkaUserTemplates.kafkaUserWithSecret(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, user, user + passwordSuffix),
                KafkaUserTemplates.kafkaUserWithSecret(deploymentNamespace, clusterName, user, user + passwordSuffix)));

        resourceManager.createResourceFromBuilderWithWait(
                KroxyliciousConfigMapTemplates.getAclRulesConfigMap(deploymentNamespace, "acl-rules", aclRules),
                KroxyliciousFilterTemplates.kroxyliciousSaslInspectorFilter(deploymentNamespace),
                KroxyliciousFilterTemplates.kroxyliciousAuthorizationFilter(deploymentNamespace, "${configmap:acl-rules:acl-rules}"));
    }

    /**
     * Deploy port identifies node with filters.
     *
     * @param clusterName the cluster name
     * @param filterNames the filter names
     */
    public void deployPortIdentifiesNodeWithFilters(String clusterName, List<String> filterNames) {
        resourceManager.createResourceFromBuilderWithWait(
                KroxyliciousKafkaProxyTemplates.defaultKafkaProxyCR(deploymentNamespace, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME, 1),
                KroxyliciousKafkaProxyIngressTemplates.defaultKafkaProxyIngressCR(deploymentNamespace, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP,
                        Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME),
                KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefCR(deploymentNamespace, clusterName),
                KroxyliciousVirtualKafkaClusterTemplates.virtualKafkaClusterWithFilterCR(deploymentNamespace, clusterName,
                        Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME,
                        clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP, filterNames));
    }

    /**
     * Deploy - Port Identifies Node with no filters config
     */
    public void deployPortIdentifiesNodeWithNoFilters(String clusterName) {
        deployPortIdentifiesNodeWithNoFilters(clusterName, 1);
    }

    /**
     * Deploy port identifies node with tls and no filters.
     *
     * @param clusterName the cluster name
     */
    public void deployPortIdentifiesNodeWithTlsAndNoFilters(String clusterName) {
        Tls tls = createCertificateConfigMapFromListener(deploymentNamespace);
        resourceManager.createResourceFromBuilder(
                KroxyliciousKafkaProxyTemplates.defaultKafkaProxyCR(deploymentNamespace, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME, 1),
                KroxyliciousKafkaProxyIngressTemplates.defaultKafkaProxyIngressCR(deploymentNamespace, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP,
                        Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME),
                KroxyliciousKafkaClusterRefTemplates.kafkaClusterRefCRWithTls(deploymentNamespace, clusterName, tls),
                KroxyliciousVirtualKafkaClusterTemplates.defaultVirtualKafkaClusterCR(deploymentNamespace, clusterName, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME,
                        clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP));
    }

    /**
     * Deploy port identifies node with downstream tls and no filters.
     *
     * @param clusterName the cluster name
     * @param tls the tls
     */
    public void deployPortIdentifiesNodeWithDownstreamTlsAndNoFilters(String clusterName, Tls tls) {
        resourceManager.createResourceFromBuilder(
                KroxyliciousKafkaProxyTemplates.defaultKafkaProxyCR(deploymentNamespace, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME, 1),
                KroxyliciousKafkaProxyIngressTemplates.tlsKafkaProxyIngressCR(deploymentNamespace, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP,
                        Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME),
                KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefCR(deploymentNamespace, clusterName),
                KroxyliciousConfigMapTemplates.getClusterCaConfigMap(deploymentNamespace, Constants.KROXYLICIOUS_TLS_CLIENT_CA_CERT, tls),
                KroxyliciousVirtualKafkaClusterTemplates.defaultVirtualKafkaClusterWithTlsCR(deploymentNamespace, clusterName, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME,
                        clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP, tls));
    }

    /**
     * Create certificate config map from listener.
     *
     * @param namespace the namespace
     * @return the tls
     */
    public Tls createCertificateConfigMapFromListener(String namespace) {
        // wait for listeners to contain data
        var tlsListenerStatus = KafkaUtils.getKafkaListenerStatus("tls");

        var cert = tlsListenerStatus.stream()
                .map(ListenerStatus::getCertificates)
                .findFirst().orElseThrow();

        resourceManager.createResourceFromBuilder(KroxyliciousConfigMapTemplates.getClusterCaConfigMap(namespace, Constants.KROXYLICIOUS_TLS_CLIENT_CA_CERT,
                cert.get(0)));
        //@formatter:off
        return new TlsBuilder()
                .withTrustAnchorRef(
                    buildTrustAnchorRef())
                .build();
        //@formatter:on
    }

    @NonNull
    private static TrustAnchorRef buildTrustAnchorRef() {
        // formatter:off
        return new TrustAnchorRefBuilder()
                .withNewRef()
                .withName(Constants.KROXYLICIOUS_TLS_CLIENT_CA_CERT)
                .withKind(Constants.CONFIG_MAP)
                .endRef()
                .withKey(Constants.KROXYLICIOUS_TLS_CA_NAME)
                .build();
        // formatter:on
    }

    /**
     * Tls config from cert.
     *
     * @param certNane the cert nane
     * @return the tls
     */
    public Tls tlsConfigFromCert(String certNane) {
        TlsBuilder tlsBuilder = new TlsBuilder();
        if (certNane != null) {
            // formatter:off
            tlsBuilder
                    .withNewCertificateRef()
                    .withName(certNane)
                    .withKind("Secret")
                    .endCertificateRef();
            // formatter:on
        }
        tlsBuilder.withTrustAnchorRef(buildTrustAnchorRef());
        return tlsBuilder.build();
    }

    /**
     * Deploy port per broker plain with record encryption filter.
     *
     * @param clusterName the cluster name
     * @param testKmsFacade the test kms facade
     */
    public void deployPortIdentifiesNodeWithRecordEncryptionFilter(String clusterName, TestKmsFacade<?, ?, ?> testKmsFacade) {
        deployPortIdentifiesNodeWithRecordEncryptionFilter(clusterName, testKmsFacade, null);
    }

    /**
     * Deploy port per broker plain with record encryption filter.
     *
     * @param clusterName the cluster name
     * @param testKmsFacade the test kms facade
     * @param experimentalKmsConfig the experimental kms config
     */
    public void deployPortIdentifiesNodeWithRecordEncryptionFilter(String clusterName, TestKmsFacade<?, ?, ?> testKmsFacade,
                                                                   ExperimentalKmsConfig experimentalKmsConfig) {
        createRecordEncryptionFilterConfigMap(testKmsFacade, experimentalKmsConfig);
        deployPortIdentifiesNodeWithFilters(clusterName, List.of(Constants.KROXYLICIOUS_ENCRYPTION_FILTER_NAME));
    }

    /**
     * Gets bootstrap on the first ingress defined on the virtual cluster.
     *
     * @param clusterName the virtual cluster name
     * @return the bootstrap server of the cluster
     */
    public String getBootstrap(String clusterName) {
        var bootstrapServer = await("poll for bootstrap").atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    var virtualKafkaCluster = kubeClient().getClient()
                            .resources(VirtualKafkaCluster.class)
                            .inNamespace(deploymentNamespace)
                            .withName(clusterName)
                            .get();

                    var first = Optional.ofNullable(virtualKafkaCluster)
                            .map(VirtualKafkaCluster::getStatus)
                            .map(VirtualKafkaClusterStatus::getIngresses)
                            .stream()
                            .flatMap(List::stream)
                            .findFirst();
                    return first.map(Ingresses::getBootstrapServer);
                }, Optional::isPresent).orElseThrow();
        LOGGER.debug("Cluster {} bootstrap server: {}", clusterName, bootstrapServer);
        return bootstrapServer;
    }

    /**
     * Gets number of replicas from the underlying deployment.
     *
     * @return the number of replicas
     */
    public int getNumberOfReplicas() {
        LOGGER.info("Getting number of replicas..");
        return await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1))
                .until(() -> Optional.ofNullable(kubeClient().getDeployment(deploymentNamespace, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME))
                        .map(Deployment::getStatus)
                        .map(DeploymentStatus::getReplicas).orElseThrow(), Objects::nonNull);
    }

    /**
     * Scale replicas to.
     *
     * @param scaledTo the number of replicas to scale up/down
     * @param timeout the timeout
     */
    public void scaleReplicasTo(int scaledTo, Duration timeout) {
        LOGGER.info("Scaling number of replicas to {}..", scaledTo);
        kubeClient().getClient().resources(KafkaProxy.class).inNamespace(deploymentNamespace).withName(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME).scale(scaledTo);
        await().atMost(timeout).pollInterval(Duration.ofSeconds(1))
                .until(() -> getNumberOfReplicas() == scaledTo && kubeClient().isDeploymentReady(deploymentNamespace, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME));
    }

    /**
     * Deploy port identifies node with no filters.
     *
     * @param clusterName the cluster name
     * @param proxyPods the proxy pods
     */
    public void deployPortIdentifiesNodeWithNoFilters(String clusterName, int proxyPods) {
        resourceManager.createResourceFromBuilder(
                KroxyliciousKafkaProxyTemplates.defaultKafkaProxyCR(deploymentNamespace, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME, proxyPods),
                KroxyliciousKafkaProxyIngressTemplates.defaultKafkaProxyIngressCR(deploymentNamespace, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP,
                        Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME),
                KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefCR(deploymentNamespace, clusterName),
                KroxyliciousVirtualKafkaClusterTemplates.defaultVirtualKafkaClusterCR(deploymentNamespace, clusterName, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME,
                        clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP));
    }

    /**
     * Deploy port identifies node with authorization filter.
     *
     * @param clusterName the cluster name
     */
    public void deployPortIdentifiesNodeWithAuthorizationFilter(String clusterName, Map<String, String> usernamePassword, List<String> aclRules) {
        createAuthorizationFilterConfigMap(clusterName, usernamePassword, aclRules);
        deployPortIdentifiesNodeWithFilters(clusterName,
                List.of(Constants.KROXYLICIOUS_SASL_INSPECTOR_FILTER_NAME, Constants.KROXYLICIOUS_AUTHORIZATION_FILTER_NAME));
    }
}
