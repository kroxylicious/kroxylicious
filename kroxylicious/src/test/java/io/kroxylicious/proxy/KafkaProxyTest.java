/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.config.ConfigParser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaProxyTest {

    @Test
    void shouldFailToStartIfRequireFilterConfigIsMissing() throws Exception {
        var config = """
                    virtualClusters:
                        demo1:
                            targetCluster:
                              bootstrap_servers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: PortPerBroker
                              config:
                                bootstrapAddress: localhost:9192
                                brokerStartPort: 9193
                                numberOfBrokerPorts: 2
                    filters:
                       - type: ProduceRequestTransformation
                """;
        try (var kafkaProxy = new KafkaProxy(new ConfigParser().parseConfiguration(config))) {
            assertThatThrownBy(kafkaProxy::startup).isInstanceOf(IllegalStateException.class).hasMessage("Missing required config for [ProduceRequestTransformation]");
        }
    }

    public static Stream<Arguments> detectsConflictingPorts() {
        return Stream.of(Arguments.of("bootstrap port conflict", """
                virtualClusters:
                  demo1:
                    targetCluster:
                      bootstrap_servers: kafka.example:1234
                    clusterNetworkAddressConfigProvider:
                      type: PortPerBroker
                      config:
                        bootstrapAddress: localhost:9192
                        numberOfBrokerPorts: 1
                  demo2:
                    targetCluster:
                      bootstrap_servers: kafka.example:1234
                    clusterNetworkAddressConfigProvider:
                      type: PortPerBroker
                      config:
                        bootstrapAddress: localhost:9192 # Conflict
                        numberOfBrokerPorts: 1
                """, "The exclusive bind of port(s) 9192,9193 to <any> would conflict with existing exclusive port bindings on <any>."),
                Arguments.of("broker port conflict", """
                        virtualClusters:
                          demo1:
                            targetCluster:
                              bootstrap_servers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: PortPerBroker
                              config:
                                bootstrapAddress: localhost:9192
                                brokerStartPort: 9193
                                numberOfBrokerPorts: 2
                          demo2:
                            targetCluster:
                              bootstrap_servers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: PortPerBroker
                              config:
                                bootstrapAddress: localhost:8192
                                brokerStartPort: 9193 # Conflict
                                numberOfBrokerPorts: 1
                        """, "The exclusive bind of port(s) 9193 to <any> would conflict with existing exclusive port bindings on <any>."));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void detectsConflictingPorts(String name, String config, String expectedMessage) throws Exception {
        try (var kafkaProxy = new KafkaProxy(new ConfigParser().parseConfiguration(config))) {
            var illegalStateException = assertThrows(IllegalStateException.class, kafkaProxy::startup);
            assertThat(illegalStateException).hasStackTraceContaining(expectedMessage);
        }
    }

    public static Stream<Arguments> missingTls() {
        return Stream.of(Arguments.of("tls mismatch", """
                virtualClusters:
                  demo1:
                    clusterNetworkAddressConfigProvider:
                      type: SniRouting
                      config:
                        bootstrapAddress: cluster1:9192
                        brokerAddressPattern:  broker-$(nodeId)
                    targetCluster:
                      bootstrap_servers: kafka.example:1234
                """, "Cluster endpoint provider requires server TLS, but this virtual cluster does not define it"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void missingTls(String name, String config, String expectedMessage) throws Exception {

        var illegalArgumentException = assertThrows(IllegalStateException.class, () -> {
            try (var kafkaProxy = new KafkaProxy(new ConfigParser().parseConfiguration(config))) {
            }
        });
        assertThat(illegalArgumentException).hasStackTraceContaining(expectedMessage);
    }
}