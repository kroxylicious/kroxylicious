/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import edu.umd.cs.findbugs.annotations.Nullable;

public class AwsKmsTestKmsFacade extends AbstractAwsKmsTestKmsFacade {
    private static final Logger LOG = LoggerFactory.getLogger(AwsKmsTestKmsFacade.class);
    private static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack:4.9.2");
    private @Nullable LocalStackContainer localStackContainer;

    @Override
    public boolean isAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Override
    @SuppressWarnings("resource")
    public void startKms() {
        localStackContainer = new LocalStackContainer(LOCALSTACK_IMAGE) {
            @Override
            @SuppressWarnings("java:S1874")
            public LocalStackContainer withFileSystemBind(String hostPath, String containerPath) {
                if (containerPath.endsWith("docker.sock")) {
                    LOG.debug("Skipped filesystem bind for {} => {}", hostPath, containerPath);
                    // Testcontainers mounts the docker.sock into the Localstack container by default.
                    // This is relied upon by only the Lambda Provider. By default, Podman prevents
                    // containers accessing the docker.sock (unless run in rootful mode). Since the
                    // Lambda Provider is not required by our use-case, skipping the filesystem bind is
                    // the simplest option.
                    // https://docs.localstack.cloud/getting-started/installation/#docker
                    // https://github.com/containers/podman/issues/6015
                    return this;
                }
                else {
                    return super.withFileSystemBind(hostPath, containerPath);
                }
            }
        }.withServices(LocalStackContainer.Service.KMS);

        localStackContainer.start();
    }

    @Override
    public void stopKms() {
        if (localStackContainer != null) {
            localStackContainer.close();
        }
    }

    @Override
    protected URI getAwsUrl() {
        return localStackContainer.getEndpointOverride(LocalStackContainer.Service.KMS);
    }

    @Override
    protected String getRegion() {
        return localStackContainer.getRegion();
    }

    @Override
    protected String getSecretKey() {
        return localStackContainer.getSecretKey();
    }

    @Override
    protected String getAccessKey() {
        return localStackContainer.getAccessKey();
    }
}
