/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
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
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class CustomResourceValidationIT {

    public static final Namespace NAMESPACE = new NamespaceBuilder().withNewMetadata().withName("proxy-ns").endMetadata().build();
    public static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    @BeforeAll
    static void beforeAll() {
        KubernetesClient client = OperatorTestUtils.kubeClient();
        LocallyRunOperatorExtension.applyCrd(KafkaProtocolFilter.class, client);
        LocallyRunOperatorExtension.applyCrd(KafkaProxy.class, client);
        LocallyRunOperatorExtension.applyCrd(VirtualKafkaCluster.class, client);
        LocallyRunOperatorExtension.applyCrd(KafkaService.class, client);
        LocallyRunOperatorExtension.applyCrd(KafkaProxyIngress.class, client);
        client.namespaces().resource(NAMESPACE).createOr(NonDeletingOperation::update);
    }

    @AfterAll
    static void afterAll() {
        try (KubernetesClient kubernetesClient = OperatorTestUtils.kubeClient()) {
            kubernetesClient.namespaces().resource(NAMESPACE).delete();
            kubernetesClient.resources(KafkaProtocolFilter.class).delete();
            kubernetesClient.resources(KafkaProxyIngress.class).delete();
            kubernetesClient.resources(KafkaProxy.class).delete();
            kubernetesClient.resources(VirtualKafkaCluster.class).delete();
            kubernetesClient.resources(KafkaService.class).delete();
        }
    }

    public static Stream<Path> testDerivedResourceInputsValid() {
        return TestFiles.recursiveFilesInDirectoryForTest(DerivedResourcesTest.class, "in-*.yaml").stream();
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
    void testDerivedResourceInputsValid(Path validYaml) {
        testValid(validYaml);
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
            NamespaceableResource<HasMetadata> resource = OperatorTestUtils.kubeClient().resource(is);
            Assertions.assertThatCode(resource::create).doesNotThrowAnyException();
            Assertions.assertThatCode(resource::delete).doesNotThrowAnyException();
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

    @MethodSource
    @ParameterizedTest
    void testResourceInvalid(InvalidResource invalidYaml) {
        NamespaceableResource<HasMetadata> resource = OperatorTestUtils.kubeClient().resource(invalidYaml.resourceAsString());
        try {
            Assertions.assertThatThrownBy(resource::create).isInstanceOfSatisfying(KubernetesClientException.class, e -> {
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
            NamespaceableResource<HasMetadata> resource = OperatorTestUtils.kubeClient().resource(is);
            Assertions.assertThatCode(resource::create).doesNotThrowAnyException();
            Assertions.assertThatCode(resource::patchStatus).doesNotThrowAnyException();
            Assertions.assertThatCode(resource::delete).doesNotThrowAnyException();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @MethodSource
    @ParameterizedTest
    void testResourceInvalidStatus(InvalidResource invalidYaml) {
        NamespaceableResource<HasMetadata> resource = OperatorTestUtils.kubeClient().resource(invalidYaml.resourceAsString());
        try {
            Assertions.assertThatCode(resource::create).doesNotThrowAnyException();
            Assertions.assertThatThrownBy(resource::patchStatus).isInstanceOfSatisfying(KubernetesClientException.class, e -> {
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
