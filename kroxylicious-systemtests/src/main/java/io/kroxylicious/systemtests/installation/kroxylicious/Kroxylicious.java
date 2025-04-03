/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.kms.ExperimentalKmsConfig;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousFilterTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaClusterRefTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyIngressTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousVirtualKafkaClusterTemplates;

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
        resourceManager.createResourceFromBuilder(
                KroxyliciousFilterTemplates.kroxyliciousRecordEncryptionFilter(deploymentNamespace, testKmsFacade, experimentalKmsConfig));
    }

    /**
     * Deploy port identifies node with filters.
     *
     * @param clusterName the cluster name
     * @param filterNames the filter names
     */
    public void deployPortIdentifiesNodeWithFilters(String clusterName, List<String> filterNames) {
        resourceManager.createResourceFromBuilder(
                KroxyliciousKafkaProxyTemplates.defaultKafkaProxyDeployment(deploymentNamespace, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME),
                KroxyliciousKafkaProxyIngressTemplates.defaultKafkaProxyIngressDeployment(deploymentNamespace, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP,
                        Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME),
                KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefDeployment(deploymentNamespace, clusterName),
                KroxyliciousVirtualKafkaClusterTemplates.virtualKafkaClusterWithFilterDeployment(deploymentNamespace, clusterName,
                        Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME,
                        clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP, filterNames));
    }

    /**
     * Deploy - Port Identifies Node with no filters config
     */
    public void deployPortIdentifiesNodeWithNoFilters(String clusterName) {
        resourceManager.createResourceFromBuilder(
                KroxyliciousKafkaProxyTemplates.defaultKafkaProxyDeployment(deploymentNamespace, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME),
                KroxyliciousKafkaProxyIngressTemplates.defaultKafkaProxyIngressDeployment(deploymentNamespace, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP,
                        Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME),
                KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefDeployment(deploymentNamespace, clusterName),
                KroxyliciousVirtualKafkaClusterTemplates.defaultVirtualKafkaClusterDeployment(deploymentNamespace, clusterName, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME,
                        clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP));
    }

    /**
     * Deploy port per broker plain with record encryption filter.
     *
     * @param clusterName the cluster name
     * @param testKmsFacade the test kms facade
     */
    public void deployPortPerBrokerPlainWithRecordEncryptionFilter(String clusterName, TestKmsFacade<?, ?, ?> testKmsFacade) {
        deployPortPerBrokerPlainWithRecordEncryptionFilter(clusterName, testKmsFacade, null);
    }

    /**
     * Deploy port per broker plain with record encryption filter.
     *
     * @param clusterName the cluster name
     * @param testKmsFacade the test kms facade
     * @param experimentalKmsConfig the experimental kms config
     */
    public void deployPortPerBrokerPlainWithRecordEncryptionFilter(String clusterName, TestKmsFacade<?, ?, ?> testKmsFacade,
                                                                   ExperimentalKmsConfig experimentalKmsConfig) {
        createRecordEncryptionFilterConfigMap(testKmsFacade, experimentalKmsConfig);
        deployPortIdentifiesNodeWithFilters(clusterName, List.of(Constants.KROXYLICIOUS_ENCRYPTION_FILTER_NAME));
    }

    /**
     * Gets bootstrap.
     *
     * @param serviceNamePrefix the service name prefix
     * @return the bootstrap
     */
    public String getBootstrap(String serviceNamePrefix) {
        String serviceName = kubeClient().getServiceNameByPrefix(deploymentNamespace, serviceNamePrefix);
        String clusterIP = kubeClient().getService(deploymentNamespace, serviceName).getSpec().getClusterIP();
        if (clusterIP == null || clusterIP.isEmpty()) {
            throw new KubeClusterException("Unable to get the clusterIP of Kroxylicious");
        }
        String bootstrap = clusterIP + ":9292";
        LOGGER.debug("Kroxylicious bootstrap: {}", bootstrap);
        return bootstrap;
    }

    /**
     * Gets number of replicas.
     *
     * @return the number of replicas
     */
    public int getNumberOfReplicas() {
        LOGGER.info("Getting number of replicas..");
        return kubeClient().getDeployment(deploymentNamespace, Constants.KROXY_DEPLOYMENT_NAME).getStatus().getReplicas();
    }

    /**
     * Scale replicas to.
     *
     * @param scaledTo the number of replicas to scale up/down
     * @param timeout the timeout
     */
    public void scaleReplicasTo(int scaledTo, Duration timeout) {
        LOGGER.info("Scaling number of replicas to {}..", scaledTo);
        kubeClient().getClient().apps().deployments().inNamespace(deploymentNamespace).withName(Constants.KROXY_DEPLOYMENT_NAME).scale(scaledTo);
        await().atMost(timeout).pollInterval(Duration.ofSeconds(1))
                .until(() -> getNumberOfReplicas() == scaledTo && kubeClient().isDeploymentReady(deploymentNamespace, Constants.KROXY_DEPLOYMENT_NAME));
    }
}
