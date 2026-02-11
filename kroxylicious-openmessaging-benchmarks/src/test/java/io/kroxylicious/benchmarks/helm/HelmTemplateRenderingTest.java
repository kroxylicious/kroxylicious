/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.helm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that verify Helm templates render correctly.
 * Tests use test-values.yaml with fixed versions for predictable assertions.
 * Expected values are loaded from test-values.yaml to avoid duplication.
 */
@EnabledIf(value = "io.kroxylicious.benchmarks.helm.HelmUtils#isHelmAvailable", disabledReason = "Helm is not installed or not available in PATH")
class HelmTemplateRenderingTest {

    // Expected values loaded from test-values.yaml
    private static String testKafkaVersion;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void setup() throws IOException {
        // Load test values from test-values.yaml to use as expected values
        ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
        String testValuesPath = "src/test/resources/test-values.yaml";
        Map<String, Object> values = yaml.readValue(Files.readString(Paths.get(testValuesPath)), Map.class);

        Map<String, Object> kafka = (Map<String, Object>) values.get("kafka");
        testKafkaVersion = (String) kafka.get("version");
    }

    @Test
    void shouldRenderWithoutErrors() {
        // When: Rendering templates with default values
        String yaml = HelmUtils.renderTemplate();

        // Then: Should produce YAML output
        assertThat(yaml)
                .as("Rendered templates should not be empty")
                .isNotEmpty();
    }

    @Test
    void shouldRenderValidKubernetesResources() throws IOException {
        // When: Rendering and parsing templates
        String yaml = HelmUtils.renderTemplate();
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);

        // Then: Should parse into Kubernetes resources
        assertThat(resources)
                .as("Should parse into valid Kubernetes resources")
                .isNotEmpty()
                .allMatch(r -> r.getKind() != null, "All resources should have a kind")
                .allMatch(r -> r.getMetadata() != null, "All resources should have metadata");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldRenderKafkaCustomResource() throws IOException {
        // When: Rendering templates and finding Kafka custom resource
        String yaml = HelmUtils.renderTemplate();
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource kafka = HelmUtils.findResourceTyped(resources, "Kafka", "kafka");

        // Then: Kafka CR should use v1 API
        assertThat(kafka).isNotNull();
        assertThat(kafka.getKind()).isEqualTo("Kafka");
        assertThat(kafka.getApiVersion()).isEqualTo("kafka.strimzi.io/v1");

        // Verify version is set
        Map<String, Object> spec = kafka.get("spec");
        Map<String, Object> kafkaSpec = (Map<String, Object>) spec.get("kafka");
        assertThat(kafkaSpec)
                .as("Kafka CR should have version specified")
                .hasEntrySatisfying("version", v -> assertThat(v).isEqualTo(testKafkaVersion));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 3, 5 })
    void shouldRenderWithConfigurableReplicaCount(int replicas) throws IOException {
        // When: Rendering with custom replica count
        String yaml = HelmUtils.renderTemplate(Map.of("kafka.replicas", String.valueOf(replicas)));
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource nodePool = HelmUtils.findResourceTyped(resources, "KafkaNodePool", "kafka-pool");

        // Then: KafkaNodePool should have configured replica count
        assertThat(nodePool).isNotNull();
        assertThat(nodePool.getApiVersion()).isEqualTo("kafka.strimzi.io/v1");
        Map<String, Object> spec = nodePool.get("spec");
        assertThat(spec).as("KafkaNodePool should have %d replicas", replicas).hasEntrySatisfying("replicas", v -> assertThat(v).isEqualTo(replicas));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldConfigureKafkaVersion() throws IOException {
        // When: Rendering templates and finding Kafka custom resource
        String yaml = HelmUtils.renderTemplate();
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource kafka = HelmUtils.findResourceTyped(resources, "Kafka", "kafka");

        // Then: Kafka CR should have version configured
        assertThat(kafka).isNotNull();
        Map<String, Object> spec = kafka.get("spec");
        Map<String, Object> kafkaSpec = (Map<String, Object>) spec.get("kafka");
        assertThat(kafkaSpec).as("Kafka CR should have version specified").containsEntry("version", testKafkaVersion);
    }

    @Test
    void shouldSetWorkersEnvironmentVariable() throws IOException {
        // When: Rendering templates
        String yaml = HelmUtils.renderTemplate();
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource benchmarkDeployment = HelmUtils.findResourceTyped(resources, "Deployment", "omb-benchmark");

        // Then: Deployment pod template should have WORKERS env var
        String workersValue = HelmUtils.getPodEnvVar(benchmarkDeployment, "WORKERS");

        assertThat(workersValue)
                .as("Benchmark deployment should have WORKERS environment variable")
                .isNotNull();
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 3, 5 })
    void shouldGenerateCorrectNumberOfWorkerUrls(int workerReplicas) throws IOException {
        // When: Rendering with custom worker replica count
        String yaml = HelmUtils.renderTemplate(Map.of("omb.workerReplicas", String.valueOf(workerReplicas)));
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource benchmarkDeployment = HelmUtils.findResourceTyped(resources, "Deployment", "omb-benchmark");

        // Then: WORKERS env var should contain URL for each replica
        String workersValue = HelmUtils.getPodEnvVar(benchmarkDeployment, "WORKERS");
        assertThat(workersValue).isNotNull();
        String[] workers = workersValue.split(",");

        assertThat(workers)
                .as("Should have worker URL for each replica")
                .hasSize(workerReplicas);
    }

    @Test
    void shouldFormatWorkerUrlsCorrectly() throws IOException {
        // When: Rendering templates with 3 workers
        String yaml = HelmUtils.renderTemplate(Map.of("omb.workerReplicas", "3"));
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource benchmarkDeployment = HelmUtils.findResourceTyped(resources, "Deployment", "omb-benchmark");

        // Then: Worker URLs should use short DNS form (same namespace)
        String workersValue = HelmUtils.getPodEnvVar(benchmarkDeployment, "WORKERS");

        assertThat(workersValue)
                .as("Worker URLs should use StatefulSet pod DNS names")
                .contains("http://omb-worker-0.omb-worker:")
                .contains("http://omb-worker-1.omb-worker:")
                .contains("http://omb-worker-2.omb-worker:");
    }

}
