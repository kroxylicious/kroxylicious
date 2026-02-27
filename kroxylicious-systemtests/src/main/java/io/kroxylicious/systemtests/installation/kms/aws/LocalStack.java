/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.aws;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;
import io.kroxylicious.systemtests.utils.TestUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type LocalStack.
 */
public class LocalStack implements AwsKmsClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalStack.class);
    public static final String LOCALSTACK_SERVICE_NAME = "localstack";
    public static final String LOCALSTACK_DEFAULT_NAMESPACE = "localstack";
    public static final String LOCALSTACK_HELM_REPOSITORY_URL = "https://localstack.github.io/helm-charts";
    public static final String LOCALSTACK_HELM_REPOSITORY_NAME = "localstack";
    public static final String LOCALSTACK_HELM_CHART_NAME = "localstack/localstack";
    private final String deploymentNamespace;

    /**
     * Instantiates a new Aws.
     *
     */
    public LocalStack() {
        this.deploymentNamespace = LOCALSTACK_DEFAULT_NAMESPACE;
    }

    @Override
    public boolean isAvailable() {
        return !Environment.USE_CLOUD_KMS.equalsIgnoreCase("true");
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
        NamespaceUtils.createNamespaceAndPrepare(deploymentNamespace);
        ResourceManager.helmClient().addRepository(LOCALSTACK_HELM_REPOSITORY_NAME, LOCALSTACK_HELM_REPOSITORY_URL);
        ResourceManager.helmClient().namespace(deploymentNamespace).install(LOCALSTACK_HELM_CHART_NAME, LOCALSTACK_SERVICE_NAME,
                Optional.empty(),
                Optional.of(Path.of(TestUtils.getResourcesURI("helm_localstack_overrides.yaml"))),
                Optional.of(Map.of("image.repository", Constants.DOCKER_REGISTRY_GCR_MIRROR + "/" + LOCALSTACK_HELM_CHART_NAME)));

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    var client = HttpClient.newHttpClient();

                    var request = HttpRequest.newBuilder()
                            .uri(getAwsKmsUrl().resolve("/notfound"))
                            .GET()
                            .build();

                    var response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    LOGGER.info("Awaiting for Localstack on Kubenetes to answer {}/{}. ", response.body(), response.statusCode());
                    if (response.statusCode() != 404) {
                        throw new RuntimeException("Localstack seem to be not ready");
                    }
                    else {
                        LOGGER.info("Endpoint seems to be minimally responsive.");
                    }
                });
    }

    @Override
    public void delete() {
        LOGGER.info("Deleting Aws in {} namespace", deploymentNamespace);
        String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();
        NamespaceUtils.deleteNamespaceWithWaitAndRemoveFromSet(deploymentNamespace, testSuiteName);
    }

    @Override
    public URI getAwsKmsUrl() {
        return URI.create("http://" + DeploymentUtils.getNodePortServiceAddress(deploymentNamespace, LOCALSTACK_SERVICE_NAME));
    }

    @Override
    public String getRegion() {
        return Environment.AWS_REGION_DEFAULT;
    }
}
