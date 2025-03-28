/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.kms.ExperimentalKmsConfig;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousConfigMapTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousDeploymentTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaClusterRefTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyIngressTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousServiceTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousVirtualKafkaClusterTemplates;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Kroxylicious.
 */
public class Kroxylicious {
    private static final Logger LOGGER = LoggerFactory.getLogger(Kroxylicious.class);
    private final String deploymentNamespace;
    private final String containerImage;
    private final ResourceManager resourceManager = ResourceManager.getInstance();

    /**
     * Instantiates a new Kroxylicious Service to be used in kubernetes.
     *
     * @param deploymentNamespace the deployment namespace
     */
    public Kroxylicious(String deploymentNamespace) {
        this.deploymentNamespace = deploymentNamespace;
        String kroxyUrl = Environment.KROXY_IMAGE_REPO + (Environment.KROXY_IMAGE_REPO.endsWith(":") ? "" : ":");
        this.containerImage = kroxyUrl + Environment.KROXY_VERSION;
    }

    private void createRecordEncryptionFilterConfigMap(String clusterName, TestKmsFacade<?, ?, ?> testKmsFacade, ExperimentalKmsConfig experimentalKmsConfig) {
        LOGGER.info("Deploy Kroxylicious config Map with record encryption filter in {} namespace", deploymentNamespace);
        resourceManager
                .createResourceWithWait(
                        KroxyliciousConfigMapTemplates.kroxyliciousRecordEncryptionConfig(clusterName, deploymentNamespace, testKmsFacade, experimentalKmsConfig)
                                .build());
    }

    private void deployPortPerBrokerPlain(int replicas) {
        LOGGER.info("Deploy Kroxylicious in {} namespace", deploymentNamespace);
        resourceManager.createResourceWithWait(KroxyliciousDeploymentTemplates.defaultKroxyDeployment(deploymentNamespace, containerImage, replicas).build());
        resourceManager.createResourceWithoutWait(KroxyliciousServiceTemplates.defaultKroxyService(deploymentNamespace).build());
    }

    /**
     * Deploy - Port Identifies Node with no filters config
     */
    public void deployPortIdentifiesNodeWithNoFilters(String clusterName) {
        KafkaProxy kafkaProxy = KroxyliciousKafkaProxyTemplates.defaultKafkaProxyDeployment(deploymentNamespace, Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME).build();
        KafkaProxyIngress kafkaProxyIngress = KroxyliciousKafkaProxyIngressTemplates.defaultKafkaProxyIngressDeployment(deploymentNamespace, "cluster-ip",
                kafkaProxy).build();
        KafkaService kafkaService = KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefDeployment(deploymentNamespace, clusterName).build();
        VirtualKafkaCluster virtualKafkaCluster = KroxyliciousVirtualKafkaClusterTemplates
                .defaultVirtualKafkaClusterDeployment(deploymentNamespace, clusterName, kafkaProxy, kafkaService, kafkaProxyIngress).build();

        resourceManager.createResourceWithWait(kafkaProxy);
        resourceManager.createResourceWithWait(kafkaProxyIngress);
        resourceManager.createResourceWithWait(kafkaService);
        resourceManager.createResourceWithWait(virtualKafkaCluster);
    }

    /**
     * Deploy port per broker plain with record encryption filter.
     *
     * @param clusterName the cluster name
     * @param replicas the replicas
     * @param testKmsFacade the test kms facade
     */
    public void deployPortPerBrokerPlainWithRecordEncryptionFilter(String clusterName, int replicas, TestKmsFacade<?, ?, ?> testKmsFacade) {
        deployPortPerBrokerPlainWithRecordEncryptionFilter(clusterName, replicas, testKmsFacade, null);
    }

    /**
     * Deploy port per broker plain with record encryption filter.
     *
     * @param clusterName the cluster name
     * @param replicas the replicas
     * @param testKmsFacade the test kms facade
     */
    public void deployPortPerBrokerPlainWithRecordEncryptionFilter(String clusterName, int replicas, TestKmsFacade<?, ?, ?> testKmsFacade,
                                                                   ExperimentalKmsConfig experimentalKmsConfig) {
        createRecordEncryptionFilterConfigMap(clusterName, testKmsFacade, experimentalKmsConfig);
        deployPortPerBrokerPlain(replicas);
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
