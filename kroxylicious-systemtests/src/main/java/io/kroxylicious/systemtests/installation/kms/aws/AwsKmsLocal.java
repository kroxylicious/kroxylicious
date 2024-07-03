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
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.ServicePort;

import io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.KafkaUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;
import io.kroxylicious.systemtests.utils.TestUtils;
import io.kroxylicious.systemtests.utils.VersionComparator;

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
    private String region;

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

    @Override
    public boolean isAvailable() {
        return Environment.AWS_ACCESS_KEY_ID.equals(Environment.AWS_ACCESS_KEY_ID_DEFAULT);
    }

    /**
     * Gets the installed version.
     *
     * @return the version
     */
    public String getLocalStackVersionInstalled() {
        if (installedLocalStackVersion == null) {
            URI url = getAwsUrl();
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
                installedLocalStackVersion = new ObjectMapper().readTree(output.toString()).findValue("version").textValue().split(":")[0];
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return installedLocalStackVersion;
    }

    private boolean isDeployed() {
        return kubeClient().getService(deploymentNamespace, LOCALSTACK_SERVICE_NAME) != null;
    }

    @Override
    public void deploy() {
        if (isDeployed()) {
            LOGGER.warn("Skipping AWS deployment. It is already deployed!");
            return;
        }

        LOGGER.info("Deploy AWS in {} namespace", deploymentNamespace);
        NamespaceUtils.createNamespaceWithWait(deploymentNamespace);
        ResourceManager.helmClient().addRepository(LOCALSTACK_HELM_REPOSITORY_NAME, LOCALSTACK_HELM_REPOSITORY_URL);
        ResourceManager.helmClient().namespace(deploymentNamespace).install(LOCALSTACK_HELM_CHART_NAME, LOCALSTACK_SERVICE_NAME,
                Optional.of(Environment.AWS_LOCALSTACK_CHART_VERSION),
                Optional.of(Path.of(TestUtils.getResourcesURI("helm_localstack_overrides.yaml"))),
                Optional.empty());

        this.podName = KafkaUtils.getPodNameByLabel(deploymentNamespace, "app.kubernetes.io/name", LOCALSTACK_SERVICE_NAME, Duration.ofSeconds(30));

        if (!isCorrectVersionInstalled()) {
            throw new KubeClusterException("AWS version installed " + getLocalStackVersionInstalled() + " does not match with the expected: '"
                    + AwsKmsTestKmsFacade.LOCALSTACK_IMAGE.getVersionPart() + "'");
        }
    }

    private boolean isCorrectVersionInstalled() {
        String installedVersion = getLocalStackVersionInstalled();
        String expectedVersion = AwsKmsTestKmsFacade.LOCALSTACK_IMAGE.getVersionPart();

        VersionComparator comparator = new VersionComparator(installedVersion);
        return comparator.compareTo(expectedVersion) == 0;
    }

    @Override
    public void delete() {
        LOGGER.info("Deleting Aws in {} namespace", deploymentNamespace);
        NamespaceUtils.deleteNamespaceWithWait(deploymentNamespace);
    }

    @Override
    public URI getAwsUrl() {
        var nodeIP = kubeClient(deploymentNamespace).getClient().nodes().list().getItems().get(0).getStatus().getAddresses().get(0).getAddress();
        var spec = kubeClient().getService(deploymentNamespace, LOCALSTACK_SERVICE_NAME).getSpec();
        int port = spec.getPorts().stream().map(ServicePort::getNodePort).findFirst()
                .orElseThrow(() -> new KubeClusterException("Unable to get the service port of Aws"));
        String url = nodeIP + ":" + port;
        LOGGER.debug("AWS URL: {}", url);
        return URI.create("http://" + url);
    }

    @Override
    public String getRegion() {
        if (region == null) {
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
                region = output.toString().trim();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return region;
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
