/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.aws;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.ServicePort;

import io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.KafkaUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;
import io.kroxylicious.systemtests.utils.TestUtils;
import io.kroxylicious.systemtests.utils.VersionComparator;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type LocalStack.
 */
public class LocalStack implements AwsKmsClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalStack.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    public static final String LOCALSTACK_SERVICE_NAME = "localstack";
    public static final String LOCALSTACK_DEFAULT_NAMESPACE = "localstack";
    public static final String LOCALSTACK_HELM_REPOSITORY_URL = "https://localstack.github.io/helm-charts";
    public static final String LOCALSTACK_HELM_REPOSITORY_NAME = "localstack";
    public static final String LOCALSTACK_HELM_CHART_NAME = "localstack/localstack";
    private static final String AWS_LOCAL_CMD = "awslocal";
    private final String deploymentNamespace;
    private String podName;
    private String installedLocalStackVersion;
    private String region;

    /**
     * Instantiates a new Aws.
     *
     */
    public LocalStack() {
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
            URI url = URI.create(getAwsUrl() + "/_" + LOCALSTACK_SERVICE_NAME + "/info");
            HttpRequest request = HttpRequest.newBuilder(url).GET().build();
            try {
                HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
                installedLocalStackVersion = OBJECT_MAPPER.readTree(response.body()).findValue("version").textValue().split(":")[0];
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted during REST API call: %s".formatted(request.uri()), e);
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
            throw new KubeClusterException("Localstack version installed " + getLocalStackVersionInstalled() + " does not match with the expected: '"
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
            List<String> command = List.of(AWS_LOCAL_CMD, "configure", "get", "region");
            ExecResult execResult = cmdKubeClient(deploymentNamespace).execInPod(podName, true, command);

            if (!execResult.isSuccess()) {
                throw new KubeClusterException("Failed to run AWS: %s, exit code: %d, stderr: %s".formatted(String.join(" ", command),
                        execResult.returnCode(), execResult.err()));
            }

            region = execResult.out().trim();
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
