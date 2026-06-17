/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.validation;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.apicurio.registry.client.RegistryClientFactory;
import io.apicurio.registry.client.common.RegistryClientOptions;
import io.apicurio.registry.rest.client.RegistryClient;

/**
 * Base class for record validation tests that require Apicurio Registry for schema validation.
 * Provides common infrastructure for Avro, JSON Schema, and Protobuf validation tests.
 */
public abstract class RecordSchemaValidationBaseIT extends RecordValidationBaseIT {

    protected static final String APICURIO_REGISTRY_IMAGE = "quay.io/apicurio/apicurio-registry:3.1.6@sha256:d0625211cebb1f58a2982df29cb0945249d8f88f37ccce9e162c0c12c2aea89e";
    protected static final String APICURIO_REGISTRY_API = "/apis/registry/v3";
    protected static final int CONTAINER_PORT = 8080;

    protected static DockerImageName apicurioRegistryDockerImageName() {
        return DockerImageName.parse(APICURIO_REGISTRY_IMAGE)
                .asCompatibleSubstituteFor(DockerImageName.parse(APICURIO_REGISTRY_IMAGE.substring(0, APICURIO_REGISTRY_IMAGE.indexOf("@"))));
    }

    protected static GenericContainer<?> startRegistryContainer() {
        DockerImageName dockerImageName = apicurioRegistryDockerImageName();

        GenericContainer<?> container = new GenericContainer<>(dockerImageName)
                .withExposedPorts(CONTAINER_PORT)
                .waitingFor(Wait.forHttp(APICURIO_REGISTRY_API + "/system/info").forStatusCode(200));

        container.start();
        return container;
    }

    protected static String registryUrl(GenericContainer<?> container) {
        return "http://" + container.getHost() + ":" + container.getMappedPort(CONTAINER_PORT) + APICURIO_REGISTRY_API;
    }

    protected static RegistryClient registryClient(String registryUrl) {
        return RegistryClientFactory.create(RegistryClientOptions.create(registryUrl));
    }

    protected static void stopContainer(GenericContainer<?> container) {
        if (container != null && container.isRunning()) {
            container.stop();
        }
    }
}
