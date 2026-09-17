/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.bootstrap.RandomBootstrapSelectionStrategy;
import io.kroxylicious.proxy.bootstrap.RoundRobinBootstrapSelectionStrategy;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for backward compatibility with deprecated targetCluster configuration format.
 * These tests verify that the deprecated inline targetCluster format continues to work
 * while users migrate to the new clusterDefinitions + target.cluster reference pattern.
 *
 * @deprecated These tests cover deprecated functionality. New tests should use clusterDefinitions.
 */
@Deprecated(since = "0.22.0", forRemoval = true)
@SuppressWarnings("removal")
class ConfigParserDeprecatedVirtualClusterTargetClusterCompatibilityTest {

    private final ConfigParser configParser = new ConfigParser();

    @Test
    void shouldSupportDeprecatedTargetClusterFormat() {
        // Given/When
        var configuration = configParser.parseConfiguration("""
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: kafka.example:1234
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """);

        // Then
        assertThat(configuration.virtualClusters())
                .singleElement()
                .satisfies(vc -> {
                    assertThat(vc.name()).isEqualTo("demo1");
                    assertThat(vc.targetCluster()).isNotNull();
                    assertThat(vc.targetCluster().bootstrapServers()).isEqualTo("kafka.example:1234");
                    assertThat(vc.target()).isNull();
                });
    }

    @Test
    void shouldSupportDeprecatedTargetClusterWithTls() {
        // Given/When
        var configuration = configParser.parseConfiguration("""
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: kafka.example:1234
                    tls:
                      trust:
                        storeFile: /tmp/trust.jks
                        storePassword:
                          password: changeit
                        storeType: JKS
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """);

        // Then
        assertThat(configuration.virtualClusters())
                .singleElement()
                .satisfies(vc -> {
                    assertThat(vc.targetCluster()).isNotNull();
                    assertThat(vc.targetCluster().tls()).isPresent();
                    assertThat(vc.targetCluster().tls().get().trust()).isNotNull();
                });
    }

    @Test
    void shouldSupportDeprecatedTargetClusterWithBootstrapServerSelection() {
        // Given/When
        var configuration = configParser.parseConfiguration("""
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: kafka-0.example:1234,kafka-1.example:1234
                    bootstrapServerSelection:
                      strategy: random
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """);

        // Then
        assertThat(configuration.virtualClusters())
                .singleElement()
                .satisfies(vc -> {
                    assertThat(vc.targetCluster()).isNotNull();
                    assertThat(vc.targetCluster().selectionStrategy())
                            .isInstanceOf(RandomBootstrapSelectionStrategy.class);
                });
    }

    @Test
    void shouldSupportDeprecatedTargetClusterWithDefaultBootstrapServerSelection() {
        // Given/When
        var configuration = configParser.parseConfiguration("""
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: kafka-0.example:1234,kafka-1.example:1234
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """);

        // Then
        assertThat(configuration.virtualClusters())
                .singleElement()
                .satisfies(vc -> {
                    var targetCluster = vc.targetCluster();
                    assertThat(targetCluster).isNotNull();
                    // Field returns null to preserve YAML fidelity
                    assertThat(targetCluster.selectionStrategy()).isNull();

                    // Verify default round-robin behavior by calling bootstrapServer() multiple times
                    var expectedServers = Set.of(
                            new HostPort("kafka-0.example", 1234),
                            new HostPort("kafka-1.example", 1234));
                    var first = targetCluster.bootstrapServer();
                    var second = targetCluster.bootstrapServer();
                    var third = targetCluster.bootstrapServer();
                    assertThat(first).isIn(expectedServers);
                    assertThat(second).isIn(expectedServers).isNotEqualTo(first);
                    assertThat(third).isEqualTo(first);
                });
    }

    @Test
    void shouldSupportDeprecatedTargetClusterWithRoundRobinBootstrapServerSelection() {
        // Given/When
        var configuration = configParser.parseConfiguration("""
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: kafka-0.example:1234,kafka-1.example:1234
                    bootstrapServerSelection:
                      strategy: round-robin
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """);

        // Then
        assertThat(configuration.virtualClusters())
                .singleElement()
                .satisfies(vc -> {
                    assertThat(vc.targetCluster()).isNotNull();
                    assertThat(vc.targetCluster().selectionStrategy())
                            .isInstanceOf(RoundRobinBootstrapSelectionStrategy.class);
                });
    }

    @Test
    void shouldRejectBothTargetClusterAndTarget() {
        // Given/When/Then
        assertThatThrownBy(() -> configParser.parseConfiguration("""
                clusterDefinitions:
                - name: my-cluster
                  bootstrapServers: kafka.example:1234
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: kafka.example:1234
                  target:
                    cluster: my-cluster
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("must specify exactly one of 'targetCluster' or 'target'");
    }

    @Test
    void shouldRejectNeitherTargetClusterNorTarget() {
        // Given/When/Then
        assertThatThrownBy(() -> configParser.parseConfiguration("""
                virtualClusters:
                - name: demo1
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("must specify exactly one of 'targetCluster' or 'target'");
    }

    @Test
    void shouldSupportDeprecatedTargetClusterTlsWithNullCredentialSupplier() {
        // Given/When - verify backward compatibility for TLS on targetCluster (no credential supplier)
        var configuration = configParser.parseConfiguration("""
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: kafka.example:1234
                    tls:
                      trust:
                        storeFile: /tmp/trust.jks
                        storePassword:
                          password: changeit
                        storeType: JKS
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """);

        // Then
        assertThat(configuration.virtualClusters())
                .singleElement()
                .satisfies(vc -> {
                    assertThat(vc.targetCluster()).isNotNull();
                    assertThat(vc.targetCluster().tls()).isPresent();
                    assertThat(vc.targetCluster().tls().get().credentialSupplier()).isNull();
                });
    }

    @Test
    void shouldSupportMixedTargetClusterAndClusterDefinitions() {
        // Given/When - verify that some VCs can use old format while others use new
        var configuration = configParser.parseConfiguration("""
                clusterDefinitions:
                - name: new-cluster
                  bootstrapServers: new-kafka.example:1234
                virtualClusters:
                - name: old-style
                  targetCluster:
                    bootstrapServers: old-kafka.example:1234
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                - name: new-style
                  target:
                    cluster: new-cluster
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9083"
                """);

        // Then
        assertThat(configuration.virtualClusters()).hasSize(2);
        assertThat(configuration.virtualClusters().get(0).targetCluster()).isNotNull();
        assertThat(configuration.virtualClusters().get(0).target()).isNull();
        assertThat(configuration.virtualClusters().get(1).targetCluster()).isNull();
        assertThat(configuration.virtualClusters().get(1).target()).isNotNull();
    }

    @Test
    void shouldDetectMissingTargetClusterBootstrapServers() {
        // Given
        assertThatThrownBy(() ->
        // When
        configParser.parseConfiguration("""
                virtualClusters:
                  - name: demo
                    targetCluster: {}
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: cluster1:9192
                """))
                // Then
                .isInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("Missing required creator property 'bootstrapServers'");
    }
}
