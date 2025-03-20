/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Service;
import io.skodjob.testframe.utils.TestFrameUtils;

import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.kms.ExperimentalKmsConfig;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousConfigMapTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousDeploymentTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousServiceTemplates;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

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

    private void createDefaultConfigMap(String clusterName) {
        LOGGER.info("Deploy Kroxylicious default config Map without filters in {} namespace", deploymentNamespace);
        resourceManager.createResourceWithWait(KroxyliciousConfigMapTemplates.defaultKroxyliciousConfig(clusterName, deploymentNamespace).build());
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
     * Deploy - Port per broker plain with no filters config
     */
    public void deployPortPerBrokerPlainWithNoFilters() {
        deployKroxyliciousExample(Constants.PATH_TO_OPERATOR_SIMPLE_FILES);
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
     * Get bootstrap
     *
     * @return the bootstrap
     */
    public String getBootstrap() {
        String clusterIP = kubeClient().getService(deploymentNamespace, Constants.KROXY_SERVICE_NAME).getSpec().getClusterIP();
        if (clusterIP == null || clusterIP.isEmpty()) {
            throw new KubeClusterException("Unable to get the clusterIP of Kroxylicious");
        }
        String bootstrap = clusterIP + ":9292";
        LOGGER.debug("Kroxylicious bootstrap: {}", bootstrap);
        return bootstrap;
    }

    /**
     * Gets bootstrap.
     *
     * @param serviceName the service name
     * @return the bootstrap
     */
    public String getBootstrap(String serviceName) {
        String clusterIP = kubeClient().getService(deploymentNamespace, serviceName).getSpec().getClusterIP();
        if (clusterIP == null || clusterIP.isEmpty()) {
            throw new KubeClusterException("Unable to get the clusterIP of Kroxylicious");
        }
        String bootstrap = clusterIP + ":9292";
        LOGGER.debug("Kroxylicious bootstrap: {}", bootstrap);
        return bootstrap;
    }

    /**
     * Gets service name.
     *
     * @param prefix the prefix
     * @return the service name
     */
    public String getServiceName(String prefix) {
        Optional<Service> service = await().alias("await service to be available")
                .atMost(Constants.GLOBAL_STATUS_TIMEOUT)
                .pollInterval(Constants.GLOBAL_POLL_INTERVAL)
                .until(() -> kubeClient().getClient().services().inNamespace(deploymentNamespace).list().getItems()
                        .stream().filter(f -> f.getMetadata().getName().contains(prefix)).findFirst(),
                        Optional::isPresent);
        if (service.isPresent()) {
            return service.get().getMetadata().getName();
        }
        else {
            throw new KubeClusterException.NotFound("Service with prefix " + prefix + " not found!");
        }
    }

    /**
     * Deploy kroxylicious example.
     *
     * @param path the path
     */
    public void deployKroxyliciousExample(String path) {
        LOGGER.info("Deploying simple Kroxylicious");
        for (File operatorFile : getExampleFiles(path)) {
            final String resourceType = operatorFile.getName().split("\\.")[1];

            if (resourceType.equals(Constants.NAMESPACE)) {
                Namespace namespace = TestFrameUtils.configFromYaml(operatorFile, Namespace.class);
                if (!NamespaceUtils.isNamespaceCreated(namespace.getMetadata().getName())) {
                    kubeClient().getClient().resource(namespace).create();
                }
            }
            else {
                try {
                    kubeClient().getClient().load(new FileInputStream(operatorFile.getAbsolutePath()))
                            .inNamespace(deploymentNamespace)
                            .create();
                }
                catch (FileNotFoundException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private static List<File> getExampleFiles(String examplePath) {
        return Arrays.stream(Objects.requireNonNull(new File(examplePath).listFiles()))
                .sorted()
                .filter(File::isFile)
                .filter(file -> file.getName().endsWith(".yaml"))
                .toList();
    }
}
