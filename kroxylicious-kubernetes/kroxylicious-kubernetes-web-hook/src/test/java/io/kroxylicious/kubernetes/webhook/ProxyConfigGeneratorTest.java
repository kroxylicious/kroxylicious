/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfigSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.kroxylicioussidecarconfigspec.FilterDefinitions;
import io.kroxylicious.kubernetes.api.v1alpha1.kroxylicioussidecarconfigspec.NodeIdRange;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyConfigGeneratorTest {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    @Test
    void generatesValidYamlWithDefaults() throws Exception {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka.example.com:9092");

        String yaml = ProxyConfigGenerator.generateConfig(spec);
        JsonNode root = YAML_MAPPER.readTree(yaml);

        // Management
        assertThat(root.path("management").path("port").asInt()).isEqualTo(ProxyConfigGenerator.DEFAULT_MANAGEMENT_PORT);
        assertThat(root.path("management").path("bindAddress").asText()).isEqualTo("0.0.0.0");

        // Virtual cluster
        JsonNode vc = root.path("virtualClusters").get(0);
        assertThat(vc.path("name").asText()).isEqualTo("sidecar");
        assertThat(vc.path("targetCluster").path("bootstrapServers").asText())
                .isEqualTo("kafka.example.com:9092");

        // Gateway
        JsonNode gw = vc.path("gateways").get(0);
        assertThat(gw.path("name").asText()).isEqualTo("local");
        JsonNode portStrategy = gw.path("portIdentifiesNode");
        assertThat(portStrategy.path("bootstrapAddress").asText()).isEqualTo("localhost:" + ProxyConfigGenerator.DEFAULT_BOOTSTRAP_PORT);
        assertThat(portStrategy.path("advertisedBrokerAddressPattern").asText()).isEqualTo("localhost");
    }

    @Test
    void usesCustomBootstrapPort() throws Exception {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka:9092");
        spec.setBootstrapPort(29092L);

        String yaml = ProxyConfigGenerator.generateConfig(spec);
        JsonNode root = YAML_MAPPER.readTree(yaml);

        JsonNode portStrategy = root.path("virtualClusters").get(0)
                .path("gateways").get(0)
                .path("portIdentifiesNode");
        assertThat(portStrategy.path("bootstrapAddress").asText()).isEqualTo("localhost:29092");
        assertThat(portStrategy.path("nodeStartPort").asInt()).isEqualTo(29093);
    }

    @Test
    void usesCustomManagementPort() throws Exception {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka:9092");
        spec.setManagementPort(8080L);

        String yaml = ProxyConfigGenerator.generateConfig(spec);
        JsonNode root = YAML_MAPPER.readTree(yaml);

        assertThat(root.path("management").path("port").asInt()).isEqualTo(8080);
    }

    @Test
    void usesCustomNodeIdRange() throws Exception {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka:9092");
        NodeIdRange nodeIdRange = new NodeIdRange();
        nodeIdRange.setStartInclusive(0L);
        nodeIdRange.setEndInclusive(9L);
        spec.setNodeIdRange(nodeIdRange);

        String yaml = ProxyConfigGenerator.generateConfig(spec);
        JsonNode root = YAML_MAPPER.readTree(yaml);

        JsonNode namedRange = root.path("virtualClusters").get(0)
                .path("gateways").get(0)
                .path("portIdentifiesNode")
                .path("nodeIdRanges").get(0);
        assertThat(namedRange.path("start").asInt()).isZero();
        assertThat(namedRange.path("end").asInt()).isEqualTo(9);
    }

    @Test
    void resolveBootstrapPortUsesDefault() {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka:9092");
        assertThat(ProxyConfigGenerator.resolveBootstrapPort(spec)).isEqualTo(ProxyConfigGenerator.DEFAULT_BOOTSTRAP_PORT);
    }

    @Test
    void resolveBootstrapPortUsesSpecified() {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka:9092");
        spec.setBootstrapPort(29092L);
        assertThat(ProxyConfigGenerator.resolveBootstrapPort(spec)).isEqualTo(29092);
    }

    @Test
    void resolveManagementPortUsesDefault() {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka:9092");
        assertThat(ProxyConfigGenerator.resolveManagementPort(spec)).isEqualTo(ProxyConfigGenerator.DEFAULT_MANAGEMENT_PORT);
    }

    @Test
    void resolveManagementPortUsesSpecified() {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka:9092");
        spec.setManagementPort(8080L);
        assertThat(ProxyConfigGenerator.resolveManagementPort(spec)).isEqualTo(8080);
    }

    @Test
    void generatedConfigIsParseable() throws Exception {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("my-kafka.ns.svc.cluster.local:9092");

        String yaml = ProxyConfigGenerator.generateConfig(spec);

        // Verify it's valid YAML with expected structure
        JsonNode root = YAML_MAPPER.readTree(yaml);
        assertThat(root.has("management")).isTrue();
        assertThat(root.has("virtualClusters")).isTrue();
        assertThat(root.path("virtualClusters").isArray()).isTrue();
        assertThat(root.path("virtualClusters")).hasSize(1);
    }

    @Test
    void includesFilterDefinitions() throws Exception {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka:9092");

        FilterDefinitions filter = new FilterDefinitions();
        filter.setName("my-filter");
        filter.setType("com.example.MyFilterFactory");
        spec.setFilterDefinitions(List.of(filter));

        String yaml = ProxyConfigGenerator.generateConfig(spec);
        JsonNode root = YAML_MAPPER.readTree(yaml);

        JsonNode filterDefs = root.path("filterDefinitions");
        assertThat(filterDefs.isArray()).isTrue();
        assertThat(filterDefs).hasSize(1);
        assertThat(filterDefs.get(0).path("name").asText()).isEqualTo("my-filter");
        assertThat(filterDefs.get(0).path("type").asText()).isEqualTo("com.example.MyFilterFactory");

        JsonNode defaultFilters = root.path("defaultFilters");
        assertThat(defaultFilters.isArray()).isTrue();
        assertThat(defaultFilters.get(0).asText()).isEqualTo("my-filter");
    }

    @Test
    void includesMultipleFilterDefinitions() throws Exception {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka:9092");

        FilterDefinitions f1 = new FilterDefinitions();
        f1.setName("filter-a");
        f1.setType("com.example.FilterA");
        FilterDefinitions f2 = new FilterDefinitions();
        f2.setName("filter-b");
        f2.setType("com.example.FilterB");
        spec.setFilterDefinitions(List.of(f1, f2));

        String yaml = ProxyConfigGenerator.generateConfig(spec);
        JsonNode root = YAML_MAPPER.readTree(yaml);

        assertThat(root.path("filterDefinitions")).hasSize(2);
        assertThat(root.path("defaultFilters")).hasSize(2);
        assertThat(root.path("defaultFilters").get(0).asText()).isEqualTo("filter-a");
        assertThat(root.path("defaultFilters").get(1).asText()).isEqualTo("filter-b");
    }

    @Test
    void omitsFiltersWhenNoneConfigured() throws Exception {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka:9092");

        String yaml = ProxyConfigGenerator.generateConfig(spec);
        JsonNode root = YAML_MAPPER.readTree(yaml);

        assertThat(root.path("filterDefinitions").isMissingNode() || root.path("filterDefinitions").isNull())
                .as("filterDefinitions should be absent when none configured")
                .isTrue();
    }

    @Test
    void includesUpstreamTlsConfig() throws Exception {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka:9093");

        String trustStorePath = "/opt/kroxylicious/tls/upstream/ca.crt";
        String yaml = ProxyConfigGenerator.generateConfig(spec, trustStorePath);
        JsonNode root = YAML_MAPPER.readTree(yaml);

        JsonNode tls = root.path("virtualClusters").get(0)
                .path("targetCluster").path("tls");
        assertThat(tls.isMissingNode()).isFalse();
        assertThat(tls.path("trust").path("storeFile").asText()).isEqualTo(trustStorePath);
        assertThat(tls.path("trust").path("storeType").asText()).isEqualTo("PEM");
    }

    @Test
    void omitsUpstreamTlsWhenPathIsNull() throws Exception {
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka:9092");

        String yaml = ProxyConfigGenerator.generateConfig(spec, null);
        JsonNode root = YAML_MAPPER.readTree(yaml);

        JsonNode tls = root.path("virtualClusters").get(0)
                .path("targetCluster").path("tls");
        assertThat(tls.isMissingNode() || tls.isNull())
                .as("TLS should be absent when no trust store path")
                .isTrue();
    }
}
