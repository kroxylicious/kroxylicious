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

    // TODO: test cases related to different interfaces.
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
                """, "conflict(s) : 9192"),
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
                        """, "conflict(s) : 9193"),
                Arguments.of("static/sniaware bootstrap port conflict", """
                        virtualClusters:
                          demo1:
                            clusterEndpointConfigProvider:
                              type: StaticCluster
                              config:
                                bootstrapAddress: localhost:9192
                          demo2:
                            clusterEndpointConfigProvider:
                              type: SniAware
                              config:
                                bootstrapAddress: localhost:9192
                                brokerAddressPattern: broker-$(nodeId)
                            keyStoreFile: /tmp/notused
                            keystorePassword: apassword
                        """, "conflict(s) : 9192"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    public void detectsConflictingPorts(String name, String config, String expectedMessage) throws Exception {

        try (var kafkaProxy = new KafkaProxy(new ConfigParser().parseConfiguration(config))) {
            var illegalStateException = assertThrows(IllegalStateException.class, kafkaProxy::startup);
            assertThat(illegalStateException.getMessage()).contains(expectedMessage);
        }
    }

    public static Stream<Arguments> missingTls() {
        return Stream.of(Arguments.of("tls mismatch", """
                virtualClusters:
                  demo1:
                    clusterEndpointConfigProvider:
                      type: SniAware
                      config:
                        bootstrapAddress: cluster1:9192
                        brokerAddressPattern:  broker-$(nodeId)
                """, "no TLS configuration is specified"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    public void missingTls(String name, String config, String expectedMessage) throws Exception {

        try (var kafkaProxy = new KafkaProxy(new ConfigParser().parseConfiguration(config))) {
            var illegalStateException = assertThrows(IllegalStateException.class, kafkaProxy::startup);
            assertThat(illegalStateException.getMessage()).contains(expectedMessage);
        }
    }

    public static Stream<Arguments> validConfig() {
        return Stream.of(
                Arguments.of("two virtual clusters using binding same port", """
                        virtualClusters:
                          demo1:
                            clusterEndpointConfigProvider:
                              type: SniAware
                              config:
                                bootstrapAddress: cluster1:9192
                                brokerAddressPattern:  broker-$(nodeId)
                            keyStoreFile: /tmo/notused
                            keystorePassword: apassword
                          demo2:
                            clusterEndpointConfigProvider:
                              type: SniAware
                              config:
                                bootstrapAddress: cluster2:9192
                                brokerAddressPattern:  broker-$(nodeId)
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