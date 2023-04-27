/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.config.ConfigParser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaProxyTest {

    public static Stream<Arguments> detectsConflictingPorts() {
        return Stream.of(Arguments.of("bootstrap port conflict", """
                virtualClusters:
                  demo1:
                    clusterEndpointConfigProvider:
                      type: StaticCluster
                      config:
                        bootstrapAddress: localhost:9192
                  demo2:
                    clusterEndpointConfigProvider:
                      type: StaticCluster
                      config:
                        bootstrapAddress: localhost:9192 # Conflict
                """, "The exclusive bind of port(s) 9192 to <any> would conflict with existing exclusive port bindings on <any>."),
                Arguments.of("broker port conflict", """
                        virtualClusters:
                          demo1:
                            clusterEndpointConfigProvider:
                              type: StaticCluster
                              config:
                                bootstrapAddress: localhost:9192
                                brokers:
                                    0: localhost:9193
                                    1: localhost:9194
                          demo2:
                            clusterEndpointConfigProvider:
                              type: StaticCluster
                              config:
                                bootstrapAddress: localhost:8192
                                brokers:
                                    0: localhost:9193  # Conflict
                                    1: localhost:8194
                        """, "The exclusive bind of port(s) 9193 to <any> would conflict with existing exclusive port bindings on <any>."),
                Arguments.of("Static/SniRouting bootstrap port conflict", """
                        virtualClusters:
                          demo1:
                            clusterEndpointConfigProvider:
                              type: StaticCluster
                              config:
                                bootstrapAddress: localhost:9192
                          demo2:
                            clusterEndpointConfigProvider:
                              type: SniRouting
                              config:
                                bootstrapAddress: localhost:9192
                                brokerAddressPattern: broker-$(nodeId)
                            keyStoreFile: /tmp/notused
                            keystorePassword: apassword
                        """, "The shared bind of port(s) 9192 to <any> would conflict with existing exclusive port bindings on <any>."));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    public void detectsConflictingPorts(String name, String config, String expectedMessage) throws Exception {
        try (var kafkaProxy = new KafkaProxy(new ConfigParser().parseConfiguration(config))) {
            var illegalStateException = assertThrows(IllegalStateException.class, kafkaProxy::startup);
            assertThat(illegalStateException).hasStackTraceContaining(expectedMessage);
        }
    }

    public static Stream<Arguments> missingTls() {
        return Stream.of(Arguments.of("tls mismatch", """
                virtualClusters:
                  demo1:
                    clusterEndpointConfigProvider:
                      type: SniRouting
                      config:
                        bootstrapAddress: cluster1:9192
                        brokerAddressPattern:  broker-$(nodeId)
                """, "Cluster endpoint provider requires tls, but this virtual cluster does not define it"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    public void missingTls(String name, String config, String expectedMessage) throws Exception {

        var illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> {
            try (var kafkaProxy = new KafkaProxy(new ConfigParser().parseConfiguration(config))) {
            }
        });
        assertThat(illegalArgumentException).hasStackTraceContaining(expectedMessage);
    }

    public static Stream<Arguments> validConfig() {
        return Stream.of(
                Arguments.of("two virtual clusters using binding same port", """
                        virtualClusters:
                          demo1:
                            clusterEndpointConfigProvider:
                              type: SniRouting
                              config:
                                bootstrapAddress: cluster1:9192
                                brokerAddressPattern:  broker-$(nodeId)-cluster1
                            keyStoreFile: /tmo/notused
                            keystorePassword: apassword
                          demo2:
                            clusterEndpointConfigProvider:
                              type: SniRouting
                              config:
                                bootstrapAddress: cluster2:9192
                                brokerAddressPattern:  broker-$(nodeId)-cluster2
                            keyStoreFile: /tmo/notused
                            keystorePassword: apassword
                        """));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    public void validConfig(String name, String config) throws Exception {

        try (var kafkaProxy = new KafkaProxy(new ConfigParser().parseConfiguration(config))) {
            assertDoesNotThrow(kafkaProxy::startup);
        }
    }
}