/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import io.fabric8.kubernetes.client.dsl.NonDeletingOperation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@EnabledIf(value = "io.kroxylicious.kubernetes.webhook.AdmissionTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class CustomResourceValidationIT {

    public static final Namespace NAMESPACE = new NamespaceBuilder().withNewMetadata().withName("webhook-validation-test").endMetadata().build();
    public static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    private static final Path CRD_PATH = Path.of(
            "../kroxylicious-admission-api/src/main/resources/META-INF/fabric8/kroxylicioussidecarconfigs.sidecar.kroxylicious.io-v1.yml");

    @BeforeAll
    static void beforeAll() {
        KubernetesClient client = AdmissionTestUtils.kubeClient();
        applyCrd(client);
        waitForCrdEstablished(client);
        client.namespaces().resource(NAMESPACE).createOr(NonDeletingOperation::update);
    }

    @AfterAll
    static void afterAll() {
        try (KubernetesClient kubernetesClient = AdmissionTestUtils.kubeClient()) {
            kubernetesClient.namespaces().resource(NAMESPACE).delete();
            deleteCrd(kubernetesClient);
        }
    }

    private static void applyCrd(KubernetesClient client) {
        try (InputStream is = Files.newInputStream(CRD_PATH)) {
            client.load(is).serverSideApply();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void waitForCrdEstablished(KubernetesClient client) {
        client.apiextensions().v1().customResourceDefinitions()
                .withName("kroxylicioussidecarconfigs.sidecar.kroxylicious.io")
                .waitUntilCondition(
                        crd -> crd != null
                                && crd.getStatus() != null
                                && crd.getStatus().getConditions() != null
                                && crd.getStatus().getConditions().stream()
                                        .anyMatch(c -> "Established".equals(c.getType())
                                                && "True".equals(c.getStatus())),
                        60, TimeUnit.SECONDS);
    }

    private static void deleteCrd(KubernetesClient client) {
        try (InputStream is = Files.newInputStream(CRD_PATH)) {
            client.load(is).delete();
        }
        catch (IOException e) {
            // Swallow exceptions during cleanup
        }
    }

    public static Stream<Path> testResourceValid() {
        return TestFiles.recursiveFilesInDirectoryForTest(CustomResourceValidationIT.class, "valid-*.yaml").stream();
    }

    @MethodSource
    @ParameterizedTest
    void testResourceValid(Path validYaml) {
        testValid(validYaml);
    }

    private static void testValid(Path validYaml) {
        try (InputStream is = Files.newInputStream(validYaml)) {
            NamespaceableResource<HasMetadata> resource = AdmissionTestUtils.kubeClient().resource(is);
            assertThatCode(resource::create).doesNotThrowAnyException();
            assertThatCode(resource::delete).doesNotThrowAnyException();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    record InvalidResource(String expectFailureMessageToContain, Object resource) {
        String resourceAsString() {
            try {
                return YAML_MAPPER.writeValueAsString(resource);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Stream<Arguments> testResourceInvalid() {
        return TestFiles.recursiveFilesInDirectoryForTest(CustomResourceValidationIT.class, "invalid-*.yaml").stream().map(p -> {
            try {
                return Arguments.argumentSet(p.toString(), YAML_MAPPER.readValue(p.toFile(), InvalidResource.class));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @MethodSource
    @ParameterizedTest
    void testResourceInvalid(InvalidResource invalidYaml) {
        NamespaceableResource<HasMetadata> resource = AdmissionTestUtils.kubeClient().resource(invalidYaml.resourceAsString());
        try {
            assertThatThrownBy(resource::create).isInstanceOfSatisfying(KubernetesClientException.class, e -> {
                Status status = e.getStatus();
                assertThat(status).isNotNull();
                assertThat(status.getCode()).isEqualTo(422);
                assertThat(status.getMessage()).contains(invalidYaml.expectFailureMessageToContain);
            });
        }
        finally {
            try {
                resource.delete();
            }
            catch (KubernetesClientException e) {
                // ignored, redundantly deleting in case the resource was accidentally valid
            }
        }
    }

    public static Stream<Path> testResourceValidStatus() {
        return TestFiles.recursiveFilesInDirectoryForTest(CustomResourceValidationIT.class, "validStatus-*.yaml").stream();
    }

    @MethodSource
    @ParameterizedTest
    void testResourceValidStatus(Path validYaml) {
        try (InputStream is = Files.newInputStream(validYaml)) {
            NamespaceableResource<HasMetadata> resource = AdmissionTestUtils.kubeClient().resource(is);
            assertThatCode(resource::create).doesNotThrowAnyException();
            assertThatCode(resource::patchStatus).doesNotThrowAnyException();
            assertThatCode(resource::delete).doesNotThrowAnyException();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Stream<Arguments> testResourceInvalidStatus() {
        return TestFiles.recursiveFilesInDirectoryForTest(CustomResourceValidationIT.class, "invalidStatus-*.yaml").stream().map(p -> {
            try {
                return Arguments.argumentSet(p.toString(), YAML_MAPPER.readValue(p.toFile(), InvalidResource.class));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @MethodSource
    @ParameterizedTest
    void testResourceInvalidStatus(InvalidResource invalidYaml) {
        NamespaceableResource<HasMetadata> resource = AdmissionTestUtils.kubeClient().resource(invalidYaml.resourceAsString());
        try {
            assertThatCode(resource::create).doesNotThrowAnyException();
            assertThatThrownBy(resource::patchStatus).isInstanceOfSatisfying(KubernetesClientException.class, e -> {
                Status status = e.getStatus();
                assertThat(status).isNotNull();
                assertThat(status.getCode()).isEqualTo(422);
                assertThat(status.getMessage()).contains(invalidYaml.expectFailureMessageToContain);
            });
        }
        finally {
            try {
                resource.delete();
            }
            catch (KubernetesClientException e) {
                // ignored, redundantly deleting in case the resource was accidentally valid
            }
        }
    }
}
