/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.validation;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.apicurio.registry.client.RegistryClientFactory;
import io.apicurio.registry.client.common.RegistryClientOptions;
import io.apicurio.registry.rest.client.RegistryClient;

import io.kroxylicious.it.BaseIT;
import io.kroxylicious.testing.integration.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class RecordValidationBaseIT extends BaseIT {

    private static final String APICURIO_REGISTRY_IMAGE = "quay.io/apicurio/apicurio-registry:3.1.6@sha256:d0625211cebb1f58a2982df29cb0945249d8f88f37ccce9e162c0c12c2aea89e";
    private static final String APICURIO_REGISTRY_API = "/apis/registry/v3";
    private static final int CONTAINER_PORT = 8080;

    public void assertThatFutureSucceeds(Future<RecordMetadata> future) {
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(5))
                .isNotNull();
    }

    public void assertThatFutureFails(Future<RecordMetadata> rejected, Class<? extends Throwable> expectedCause, String expectedMessage) {
        assertThat(rejected)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(expectedCause)
                .withMessageContaining(expectedMessage);
    }

    public ConsumerRecords<String, String> consumeAll(KroxyliciousTester tester, Topic topic) {
        try (var consumer = tester.consumer(Map.of(GROUP_ID_CONFIG, UUID.randomUUID().toString(), AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            consumer.subscribe(Set.of(topic.name()));
            return consumer.poll(Duration.ofSeconds(10));
        }
    }

    protected static GenericContainer<?> startRegistryContainer() {
        String image = APICURIO_REGISTRY_IMAGE;
        DockerImageName dockerImageName = DockerImageName.parse(image)
                .asCompatibleSubstituteFor(DockerImageName.parse(image.substring(0, image.indexOf("@"))));

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

    static boolean isDockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }
}
