/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.helm;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests that verify Helm templates render correctly.
 */
class HelmTemplateRenderingTest {

    @BeforeAll
    static void checkHelmAvailable() {
        assumeTrue(HelmUtils.isHelmAvailable(), "Helm is not installed or not available in PATH");
    }

    @Test
    void shouldRenderWithoutErrors() throws IOException {
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
        List<Map<String, Object>> resources = HelmUtils.parseKubernetesManifests(yaml);

        // Then: Should parse into Kubernetes resources
        assertThat(resources)
                .as("Should parse into valid Kubernetes resources")
                .isNotEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldRenderKafkaStatefulSet() throws IOException {
        // When: Rendering templates and finding Kafka StatefulSet
        String yaml = HelmUtils.renderTemplate();
        List<Map<String, Object>> resources = HelmUtils.parseKubernetesManifests(yaml);
        Map<String, Object> kafkaStatefulSet = HelmUtils.findResource(resources, "StatefulSet", "kafka");

        // Then: Kafka StatefulSet should have default replica count
        assertThat(kafkaStatefulSet).isNotNull();
        Map<String, Object> spec = (Map<String, Object>) kafkaStatefulSet.get("spec");
        assertThat(spec.get("replicas"))
                .as("Kafka StatefulSet should have default 3 replicas")
                .isEqualTo(3);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 3, 5 })
    @SuppressWarnings("unchecked")
    void shouldRenderWithConfigurableReplicaCount(int replicas) throws IOException {
        // When: Rendering with custom replica count
        String yaml = HelmUtils.renderTemplate(Map.of("kafka.replicas", String.valueOf(replicas)));
        List<Map<String, Object>> resources = HelmUtils.parseKubernetesManifests(yaml);
        Map<String, Object> kafkaStatefulSet = HelmUtils.findResource(resources, "StatefulSet", "kafka");

        // Then: StatefulSet should have configured replica count
        assertThat(kafkaStatefulSet).isNotNull();
        Map<String, Object> spec = (Map<String, Object>) kafkaStatefulSet.get("spec");
        assertThat(spec.get("replicas"))
                .as("Kafka StatefulSet should have %d replicas", replicas)
                .isEqualTo(replicas);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 3, 5 })
    @SuppressWarnings("unchecked")
    void shouldGenerateCorrectControllerQuorumVoters(int replicas) throws IOException {
        // When: Rendering with custom replica count
        String yaml = HelmUtils.renderTemplate(Map.of("kafka.replicas", String.valueOf(replicas)));
        List<Map<String, Object>> resources = HelmUtils.parseKubernetesManifests(yaml);
        Map<String, Object> kafkaStatefulSet = HelmUtils.findResource(resources, "StatefulSet", "kafka");

        // Then: KAFKA_CONTROLLER_QUORUM_VOTERS should be correctly formatted
        assertThat(kafkaStatefulSet).isNotNull();
        String quorumVoters = HelmUtils.extractEnvVar(kafkaStatefulSet, "KAFKA_CONTROLLER_QUORUM_VOTERS");
        String expectedVoters = HelmUtils.generateExpectedQuorumVoters(replicas);
        assertThat(quorumVoters)
                .as("Controller quorum voters should be correctly formatted for %d replicas", replicas)
                .isEqualTo(expectedVoters);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldSetPodSecurityContext() throws IOException {
        // When: Rendering templates
        String yaml = HelmUtils.renderTemplate();
        List<Map<String, Object>> resources = HelmUtils.parseKubernetesManifests(yaml);
        Map<String, Object> kafkaStatefulSet = HelmUtils.findResource(resources, "StatefulSet", "kafka");

        // Then: Pod security context should be configured correctly
        assertThat(kafkaStatefulSet).isNotNull();
        Map<String, Object> spec = (Map<String, Object>) kafkaStatefulSet.get("spec");
        Map<String, Object> template = (Map<String, Object>) spec.get("template");
        Map<String, Object> podSpec = (Map<String, Object>) template.get("spec");
        Map<String, Object> securityContext = (Map<String, Object>) podSpec.get("securityContext");

        assertThat(securityContext.get("runAsNonRoot"))
                .as("Pod should run as non-root")
                .isEqualTo(true);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldSetContainerSecurityContext() throws IOException {
        // When: Rendering templates
        String yaml = HelmUtils.renderTemplate();
        List<Map<String, Object>> resources = HelmUtils.parseKubernetesManifests(yaml);
        Map<String, Object> kafkaStatefulSet = HelmUtils.findResource(resources, "StatefulSet", "kafka");

        // Then: Container security context should drop all capabilities
        assertThat(kafkaStatefulSet).isNotNull();
        Map<String, Object> spec = (Map<String, Object>) kafkaStatefulSet.get("spec");
        Map<String, Object> template = (Map<String, Object>) spec.get("template");
        Map<String, Object> podSpec = (Map<String, Object>) template.get("spec");
        List<Map<String, Object>> containers = (List<Map<String, Object>>) podSpec.get("containers");
        Map<String, Object> kafkaContainer = containers.get(0);
        Map<String, Object> containerSecurityContext = (Map<String, Object>) kafkaContainer.get("securityContext");

        assertThat(containerSecurityContext.get("allowPrivilegeEscalation"))
                .as("Container should not allow privilege escalation")
                .isEqualTo(false);
    }
}
