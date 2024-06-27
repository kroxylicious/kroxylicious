/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.aws;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.ServicePort;

import io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.KafkaUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;
import io.kroxylicious.systemtests.utils.TestUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Aws kms local.
 */
public class AwsKmsLocal implements AwsKmsClient {
    public static final String LOCALSTACK_SERVICE_NAME = "localstack";
    public static final String LOCALSTACK_DEFAULT_NAMESPACE = "localstack";
    public static final String LOCALSTACK_HELM_REPOSITORY_URL = "https://localstack.github.io/helm-charts";
    public static final String LOCALSTACK_HELM_REPOSITORY_NAME = "localstack";
    public static final String LOCALSTACK_HELM_CHART_NAME = "localstack/localstack";
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsKmsLocal.class);
    private static final String AWS_LOCAL_CMD = "awslocal";
    private final String deploymentNamespace;
    private String podName;
    private String installedLocalStackVersion;

    /**
     * Instantiates a new Aws.
     *
     */
    public AwsKmsLocal() {
        this.deploymentNamespace = LOCALSTACK_DEFAULT_NAMESPACE;
    }

    @Override
    public String getAwsCmd() {
        return AWS_LOCAL_CMD;
    }

    /**
     * Is deployed
     *
     * @return true if Aws service is deployed in kubernetes, false otherwise
     */
    public boolean isDeployed() {
        return kubeClient().getService(deploymentNamespace, LOCALSTACK_SERVICE_NAME) != null;
    }

    @Override
    public boolean isAvailable() {
        if (!isDeployed()) {
            return false;
        }
        try (var output = new ByteArrayOutputStream();
                var exec = kubeClient().getClient().pods()
                        .inNamespace(deploymentNamespace)
                        .withName(podName)
                        .writingOutput(output)
                        .exec("sh", "-c", AWS_LOCAL_CMD + " --version")) {
            int exitCode = exec.exitCode().join();
            return exitCode == 0 &&
                    output.toString().toLowerCase().contains("aws-cli/");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Gets the installed version.
     *
     * @return the version
     */
    public String getLocalStackVersionInstalled() {
        if (installedLocalStackVersion != null) {
            return installedLocalStackVersion;
        }

        URI url = URI.create("http://" + getAwsUrl());
        try (var output = new ByteArrayOutputStream();
                var error = new ByteArrayOutputStream();
                var exec = kubeClient().getClient().pods()
                        .inNamespace(deploymentNamespace)
                        .withName(podName)
                        .writingOutput(output)
                        .writingError(error)
                        .exec("sh", "-c", "curl " + url + "/_" + LOCALSTACK_SERVICE_NAME + "/info")) {
            int exitCode = exec.exitCode().join();
            if (exitCode != 0) {
                throw new UnsupportedOperationException(error.toString());
            }
            // version returned with format: "3.5.1.dev:6e7ddd05e"
            return new ObjectMapper().readTree(output.toString()).findValue("version").textValue().split(":")[0];
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void deploy() {
        LOGGER.info("Deploy AWS in {} namespace", deploymentNamespace);
        if (isDeployed()) {
            LOGGER.warn("Skipping AWS deployment. It is already deployed!");
            return;
        }

        NamespaceUtils.createNamespaceWithWait(deploymentNamespace);
        ResourceManager.helmClient().addRepository(LOCALSTACK_HELM_REPOSITORY_NAME, LOCALSTACK_HELM_REPOSITORY_URL);
        ResourceManager.helmClient().namespace(deploymentNamespace).install(LOCALSTACK_HELM_CHART_NAME, LOCALSTACK_SERVICE_NAME,
                Optional.of(Environment.LOCALSTACK_CHART_VERSION),
                Optional.of(Path.of(TestUtils.getResourcesURI("helm_localstack_overrides.yaml"))),
                Optional.empty());

        this.podName = KafkaUtils.getPodNameByLabel(deploymentNamespace, "app.kubernetes.io/name", LOCALSTACK_SERVICE_NAME, Duration.ofSeconds(30));
        DeploymentUtils.waitForDeploymentRunning(deploymentNamespace, podName, Duration.ofMinutes(1));

        if (!isCorrectVersionInstalled()) {
            throw new KubeClusterException("AWS version installed " + getLocalStackVersionInstalled() + " does not match with the expected: '"
                    + AwsKmsTestKmsFacade.LOCALSTACK_IMAGE.getVersionPart() + "'");
        }
    }

    private boolean isCorrectVersionInstalled() {
        String installedVersion = getLocalStackVersionInstalled();
        String expectedVersion = AwsKmsTestKmsFacade.LOCALSTACK_IMAGE.getVersionPart();

        return compareVersions(installedVersion, expectedVersion) == 0;
    }

    private int compareVersions(String currentVersion, String expectedVersion) {
        Objects.requireNonNull(expectedVersion);

        String[] currentParts = currentVersion.split("\\.");
        String[] expectedParts = expectedVersion.split("\\.");

        for (int i = 0; i < expectedParts.length; i++) {
            int currentPart = i < currentParts.length ? Integer.parseInt(currentParts[i]) : 0;
            int expectedPart = Integer.parseInt(expectedParts[i]);
            int comparison = Integer.compare(currentPart, expectedPart);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    @Override
    public void delete() {
        LOGGER.info("Deleting Aws in {} namespace", deploymentNamespace);
        NamespaceUtils.deleteNamespaceWithWait(deploymentNamespace);
    }

    @Override
    public String getAwsUrl() {
        // export NODE_PORT=$(kubectl get --namespace "localstack" -o jsonpath="{.spec.ports[0].nodePort}" services localstack)
        // export NODE_IP=$(kubectl get nodes --namespace "localstack" -o jsonpath="{.items[0].status.addresses[0].address}")
        // echo http://$NODE_IP:$NODE_PORT
        var nodeIP = kubeClient(deploymentNamespace).getClient().nodes().list().getItems().get(0).getStatus().getAddresses().get(0).getAddress();
        var spec = kubeClient().getService(deploymentNamespace, LOCALSTACK_SERVICE_NAME).getSpec();
        int port = spec.getPorts().stream().map(ServicePort::getNodePort).findFirst()
                .orElseThrow(() -> new KubeClusterException("Unable to get the service port of Aws"));
        String url = nodeIP + ":" + port;
        LOGGER.debug("AWS URL: {}", url);
        return url;
    }

    @Override
    public String getRegion() {
        try (var output = new ByteArrayOutputStream();
                var error = new ByteArrayOutputStream();
                var exec = kubeClient().getClient().pods()
                        .inNamespace(deploymentNamespace)
                        .withName(podName)
                        .writingOutput(output)
                        .writingError(error)
                        .exec("sh", "-c", AWS_LOCAL_CMD + " configure get region")) {
            int exitCode = exec.exitCode().join();
            if (exitCode != 0) {
                throw new UnsupportedOperationException(error.toString());
            }
            return output.toString().trim();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Gets pod name.
     *
     * @return the pod name
     */
    public String getPodName() {
        return podName;
    }
}
