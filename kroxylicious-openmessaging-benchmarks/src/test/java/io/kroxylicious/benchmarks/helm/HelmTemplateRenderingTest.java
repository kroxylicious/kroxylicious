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
    void shouldRenderKafkaCustomResource() throws IOException {
        // When: Rendering templates and finding Kafka custom resource
        String yaml = HelmUtils.renderTemplate();
        List<Map<String, Object>> resources = HelmUtils.parseKubernetesManifests(yaml);
        Map<String, Object> kafka = HelmUtils.findResource(resources, "Kafka", "kafka");

        // Then: Kafka CR should have default replica count
        assertThat(kafka).isNotNull();
        Map<String, Object> spec = (Map<String, Object>) kafka.get("spec");
        Map<String, Object> kafkaSpec = (Map<String, Object>) spec.get("kafka");
        assertThat(kafkaSpec.get("replicas"))
                .as("Kafka CR should have default 3 replicas")
                .isEqualTo(3);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 3, 5 })
    @SuppressWarnings("unchecked")
    void shouldRenderWithConfigurableReplicaCount(int replicas) throws IOException {
        // When: Rendering with custom replica count
        String yaml = HelmUtils.renderTemplate(Map.of("kafka.replicas", String.valueOf(replicas)));
        List<Map<String, Object>> resources = HelmUtils.parseKubernetesManifests(yaml);
        Map<String, Object> kafka = HelmUtils.findResource(resources, "Kafka", "kafka");

        // Then: Kafka CR should have configured replica count
        assertThat(kafka).isNotNull();
        Map<String, Object> spec = (Map<String, Object>) kafka.get("spec");
        Map<String, Object> kafkaSpec = (Map<String, Object>) spec.get("kafka");
        assertThat(kafkaSpec.get("replicas"))
                .as("Kafka CR should have %d replicas", replicas)
                .isEqualTo(replicas);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldConfigureKafkaVersion() throws IOException {
        // When: Rendering templates and finding Kafka custom resource
        String yaml = HelmUtils.renderTemplate();
        List<Map<String, Object>> resources = HelmUtils.parseKubernetesManifests(yaml);
        Map<String, Object> kafka = HelmUtils.findResource(resources, "Kafka", "kafka");

        // Then: Kafka CR should have version configured
        assertThat(kafka).isNotNull();
        Map<String, Object> spec = (Map<String, Object>) kafka.get("spec");
        Map<String, Object> kafkaSpec = (Map<String, Object>) spec.get("kafka");
        assertThat(kafkaSpec.get("version"))
                .as("Kafka CR should have version specified")
                .isEqualTo("4.1.1");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldSetPodSecurityContext() throws IOException {
        // When: Rendering templates
        String yaml = HelmUtils.renderTemplate();
        List<Map<String, Object>> resources = HelmUtils.parseKubernetesManifests(yaml);
        Map<String, Object> kafka = HelmUtils.findResource(resources, "Kafka", "kafka");

        // Then: Pod security context should be configured correctly
        assertThat(kafka).isNotNull();
        Map<String, Object> spec = (Map<String, Object>) kafka.get("spec");
        Map<String, Object> kafkaSpec = (Map<String, Object>) spec.get("kafka");
        Map<String, Object> template = (Map<String, Object>) kafkaSpec.get("template");
        Map<String, Object> pod = (Map<String, Object>) template.get("pod");
        Map<String, Object> securityContext = (Map<String, Object>) pod.get("securityContext");

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
        Map<String, Object> kafka = HelmUtils.findResource(resources, "Kafka", "kafka");

        // Then: Container security context should drop all capabilities
        assertThat(kafka).isNotNull();
        Map<String, Object> spec = (Map<String, Object>) kafka.get("spec");
        Map<String, Object> kafkaSpec = (Map<String, Object>) spec.get("kafka");
        Map<String, Object> template = (Map<String, Object>) kafkaSpec.get("template");
        Map<String, Object> kafkaContainer = (Map<String, Object>) template.get("kafkaContainer");
        Map<String, Object> containerSecurityContext = (Map<String, Object>) kafkaContainer.get("securityContext");

        assertThat(containerSecurityContext.get("allowPrivilegeEscalation"))
                .as("Container should not allow privilege escalation")
                .isEqualTo(false);
    }
}
