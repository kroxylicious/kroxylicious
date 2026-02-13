/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.helm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
        Map<String, Object> kafkaSpec = HelmUtils.getNestedMap(kafka.get("spec"), "kafka");
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
    void shouldConfigureKafkaVersion() throws IOException {
        // When: Rendering templates and finding Kafka custom resource
        String yaml = HelmUtils.renderTemplate();
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource kafka = HelmUtils.findResourceTyped(resources, "Kafka", "kafka");

        // Then: Kafka CR should have version configured
        assertThat(kafka).isNotNull();
        Map<String, Object> kafkaSpec = HelmUtils.getNestedMap(kafka.get("spec"), "kafka");
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
    void shouldDefaultToProductionDurations() throws IOException {
        // When: Rendering templates with default values
        String yaml = HelmUtils.renderTemplate();
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource workloadConfigMap = HelmUtils.findResourceTyped(resources, "ConfigMap", "omb-workload-1topic-1kb");

        // Then: Workload should have production-quality durations (reliable up to p99.9)
        assertThat(workloadConfigMap).as("Workload ConfigMap should exist").isNotNull();
        Map<String, Object> data = workloadConfigMap.get("data");
        assertThat(data).isNotNull();

        String workloadYaml = (String) data.get("workload.yaml");
        assertThat(workloadYaml)
                .as("Default test duration should be 15 minutes (sufficient for p99.9)")
                .contains("testDurationMinutes: 15");
        assertThat(workloadYaml)
                .as("Default warmup duration should be 5 minutes")
                .contains("warmupDurationMinutes: 5");
        assertThat(workloadYaml)
                .as("Default producer rate should be 50000 for 1-topic workload")
                .contains("producerRate: 50000");
    }

    @Test
    void smokeProfileShouldOverrideDurations() throws IOException {
        String workloadYaml = renderSmokeWorkloadYaml();
        assertThat(workloadYaml)
                .contains("testDurationMinutes: 1")
                .contains("warmupDurationMinutes: 0");
    }

    @Test
    void smokeProfileShouldReduceProducerRate() throws IOException {
        String workloadYaml = renderSmokeWorkloadYaml();
        assertThat(workloadYaml)
                .as("Smoke producer rate should be reduced to avoid overwhelming a single broker")
                .contains("producerRate: 10000");
    }

    @Test
    void smokeProfileShouldUseSingleBroker() throws IOException {
        List<GenericKubernetesResource> resources = renderSmokeResources();
        GenericKubernetesResource nodePool = HelmUtils.findResourceTyped(resources, "KafkaNodePool", "kafka-pool");
        assertThat(nodePool).isNotNull();
        Map<String, Object> nodePoolSpec = nodePool.get("spec");
        assertThat(nodePoolSpec).hasEntrySatisfying("replicas", v -> assertThat(v).isEqualTo(1));
    }

    @Test
    void smokeProfileShouldSetReplicationFactorToOne() throws IOException {
        List<GenericKubernetesResource> resources = renderSmokeResources();
        GenericKubernetesResource kafka = HelmUtils.findResourceTyped(resources, "Kafka", "kafka");
        Map<String, Object> kafkaConfig = HelmUtils.getNestedMap(kafka.get("spec"), "kafka", "config");
        assertThat(kafkaConfig)
                .containsEntry("default.replication.factor", 1)
                .containsEntry("offsets.topic.replication.factor", 1)
                .containsEntry("transaction.state.log.replication.factor", 1)
                .containsEntry("min.insync.replicas", 1)
                .containsEntry("transaction.state.log.min.isr", 1);
    }

    @Test
    void smokeProfileShouldUseMinimumWorkers() throws IOException {
        List<GenericKubernetesResource> resources = renderSmokeResources();
        GenericKubernetesResource benchmark = HelmUtils.findResourceTyped(resources, "Deployment", "omb-benchmark");
        String workersValue = HelmUtils.getPodEnvVar(benchmark, "WORKERS");
        assertThat(workersValue.split(",")).as("DistributedWorkersEnsemble requires at least 2 workers").hasSize(2);
    }

    @Test
    void smokeProfileShouldSetDriverReplicationToMatchBrokerCount() throws IOException {
        List<GenericKubernetesResource> resources = renderSmokeResources();
        GenericKubernetesResource driverConfigMap = HelmUtils.findResourceTyped(resources, "ConfigMap", "omb-driver-baseline");
        assertThat(driverConfigMap).isNotNull();
        Map<String, Object> driverData = driverConfigMap.get("data");
        String driverYaml = (String) driverData.get("driver-kafka.yaml");
        assertThat(driverYaml)
                .contains("replicationFactor: 1")
                .contains("min.insync.replicas=1");
    }

    private String renderSmokeWorkloadYaml() throws IOException {
        List<GenericKubernetesResource> resources = renderSmokeResources();
        GenericKubernetesResource workloadConfigMap = HelmUtils.findResourceTyped(resources, "ConfigMap", "omb-workload-1topic-1kb");
        assertThat(workloadConfigMap).as("Workload ConfigMap should exist").isNotNull();
        Map<String, Object> data = workloadConfigMap.get("data");
        return (String) data.get("workload.yaml");
    }

    private List<GenericKubernetesResource> renderSmokeResources() throws IOException {
        Path smokeValues = Paths.get("helm/kroxylicious-benchmark/scenarios/smoke-values.yaml").toAbsolutePath();
        String yaml = HelmUtils.renderTemplate(List.of(smokeValues), Map.of());
        return HelmUtils.parseKubernetesResourcesTyped(yaml);
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

    @Test
    void shouldRenderKafkaProxyWhenEnabled() throws IOException {
        // When: Rendering with kroxylicious enabled
        String yaml = HelmUtils.renderTemplate(Map.of("kroxylicious.enabled", "true"));
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource kafkaProxy = HelmUtils.findResourceTyped(resources, "KafkaProxy", "benchmark-proxy");

        // Then: KafkaProxy CR should exist with correct API version
        assertThat(kafkaProxy).as("KafkaProxy 'benchmark-proxy' should be rendered when kroxylicious is enabled").isNotNull();
        assertThat(kafkaProxy.getApiVersion()).isEqualTo("kroxylicious.io/v1alpha1");
    }

    @Test
    void shouldNotRenderKroxyliciousResourcesWhenDisabled() throws IOException {
        // When: Rendering with default values (kroxylicious disabled)
        String yaml = HelmUtils.renderTemplate();
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);

        // Then: No Kroxylicious CRs should be rendered
        assertThat(resources)
                .as("No Kroxylicious CRs should be rendered when disabled")
                .noneMatch(r -> r.getApiVersion() != null && r.getApiVersion().startsWith("kroxylicious.io/"));
    }

    @Test
    void shouldRenderKafkaProxyIngressWhenEnabled() throws IOException {
        // When: Rendering with kroxylicious enabled
        String yaml = HelmUtils.renderTemplate(Map.of("kroxylicious.enabled", "true"));
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource ingress = HelmUtils.findResourceTyped(resources, "KafkaProxyIngress", "cluster-ip");

        // Then: KafkaProxyIngress CR should exist with correct API version
        assertThat(ingress).as("KafkaProxyIngress 'cluster-ip' should be rendered when kroxylicious is enabled").isNotNull();
        assertThat(ingress.getApiVersion()).isEqualTo("kroxylicious.io/v1alpha1");
    }

    @Test
    void shouldRenderKafkaServiceWhenEnabled() throws IOException {
        // When: Rendering with kroxylicious enabled
        String yaml = HelmUtils.renderTemplate(Map.of("kroxylicious.enabled", "true"));
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource kafkaService = HelmUtils.findResourceTyped(resources, "KafkaService", "kafka");

        // Then: KafkaService CR should exist with correct API version
        assertThat(kafkaService).as("KafkaService 'kafka' should be rendered when kroxylicious is enabled").isNotNull();
        assertThat(kafkaService.getApiVersion()).isEqualTo("kroxylicious.io/v1alpha1");
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 3, 5 })
    @SuppressWarnings("unchecked")
    void shouldConfigureNodeIdRangesForReplicaCount(int replicas) throws IOException {
        // When: Rendering with kroxylicious enabled and custom replica count
        String yaml = HelmUtils.renderTemplate(Map.of("kroxylicious.enabled", "true", "kafka.replicas", String.valueOf(replicas)));
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource kafkaService = HelmUtils.findResourceTyped(resources, "KafkaService", "kafka");

        // Then: nodeIdRanges end should be replicas - 1
        assertThat(kafkaService).isNotNull();
        Map<String, Object> spec = kafkaService.get("spec");
        List<Map<String, Object>> nodeIdRanges = (List<Map<String, Object>>) spec.get("nodeIdRanges");
        assertThat(nodeIdRanges).hasSize(1);
        assertThat(nodeIdRanges.get(0))
                .as("nodeIdRanges end should be %d for %d replicas", replicas - 1, replicas)
                .containsEntry("start", 0)
                .containsEntry("end", replicas - 1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldRenderVirtualKafkaClusterWhenEnabled() throws IOException {
        // When: Rendering with kroxylicious enabled
        String yaml = HelmUtils.renderTemplate(Map.of("kroxylicious.enabled", "true"));
        List<GenericKubernetesResource> resources = HelmUtils.parseKubernetesResourcesTyped(yaml);
        GenericKubernetesResource vkc = HelmUtils.findResourceTyped(resources, "VirtualKafkaCluster", "kafka");

        // Then: VirtualKafkaCluster CR should exist with correct references and no filters
        assertThat(vkc).as("VirtualKafkaCluster 'kafka' should be rendered when kroxylicious is enabled").isNotNull();
        assertThat(vkc.getApiVersion()).isEqualTo("kroxylicious.io/v1alpha1");

        Map<String, Object> spec = vkc.get("spec");
        Map<String, Object> proxyRef = (Map<String, Object>) spec.get("proxyRef");
        assertThat(proxyRef).containsEntry("name", "benchmark-proxy");

        Map<String, Object> targetRef = (Map<String, Object>) spec.get("targetKafkaServiceRef");
        assertThat(targetRef).containsEntry("name", "kafka");

        assertThat(spec).as("No filters should be configured for no-filters scenario").doesNotContainKey("filterRefs");
    }

}
