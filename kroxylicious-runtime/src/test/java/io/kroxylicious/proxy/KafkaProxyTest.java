/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class KafkaProxyTest {

    @Test
    void shouldFailToStartIfRequireFilterConfigIsMissing() throws Exception {
        var config = """
                   virtualClusters:
                     demo1:
                       targetCluster:
                         bootstrap_servers: kafka.example:1234
                       clusterNetworkAddressConfigProvider:
                         type: PortPerBrokerClusterNetworkAddressConfigProvider
                         config:
                           bootstrapAddress: localhost:9192
                           numberOfBrokerPorts: 1
                   filters:
                   - type: RequiresConfigFactory
                """;
        var configParser = new ConfigParser();
        try (var kafkaProxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            assertThatThrownBy(kafkaProxy::startup).isInstanceOf(PluginConfigurationException.class)
                    .hasMessage(
                            "Exception initializing filter factory RequiresConfigFactory with config null: RequiresConfigFactory requires configuration, but config object is null");
        }
    }

    public static Stream<Arguments> detectsConflictingPorts() {
        return Stream.of(Arguments.of("bootstrap port conflict", """
                virtualClusters:
                  demo1:
                    targetCluster:
                      bootstrap_servers: kafka.example:1234
                    clusterNetworkAddressConfigProvider:
                      type: PortPerBrokerClusterNetworkAddressConfigProvider
                      config:
                        bootstrapAddress: localhost:9192
                        numberOfBrokerPorts: 1
                  demo2:
                    targetCluster:
                      bootstrap_servers: kafka.example:1234
                    clusterNetworkAddressConfigProvider:
                      type: PortPerBrokerClusterNetworkAddressConfigProvider
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
                              type: PortPerBrokerClusterNetworkAddressConfigProvider
                              config:
                                bootstrapAddress: localhost:9192
                                brokerStartPort: 9193
                                numberOfBrokerPorts: 2
                          demo2:
                            targetCluster:
                              bootstrap_servers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: PortPerBrokerClusterNetworkAddressConfigProvider
                              config:
                                bootstrapAddress: localhost:8192
                                brokerStartPort: 9193 # Conflict
                                numberOfBrokerPorts: 1
                        """, "The exclusive bind of port(s) 9193 to <any> would conflict with existing exclusive port bindings on <any>."));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void detectsConflictingPorts(String name, String config, String expectedMessage) throws Exception {
        ConfigParser configParser = new ConfigParser();
        try (var kafkaProxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            var illegalStateException = assertThrows(IllegalStateException.class, kafkaProxy::startup);
            assertThat(illegalStateException).hasStackTraceContaining(expectedMessage);
        }
    }

    public static Stream<Arguments> missingTls() {
        return Stream.of(Arguments.of("tls mismatch", """
                virtualClusters:
                  demo1:
                    clusterNetworkAddressConfigProvider:
                      type: SniRoutingClusterNetworkAddressConfigProvider
                      config:
                        bootstrapAddress: cluster1:9192
                        brokerAddressPattern:  broker-$(nodeId)
                    targetCluster:
                      bootstrap_servers: kafka.example:1234
                """, "Cluster endpoint provider requires server TLS, but this virtual cluster does not define it"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void missingTls(String name, String config, String expectedMessage) {

        ConfigParser configParser = new ConfigParser();
        final Configuration parsedConfiguration = configParser.parseConfiguration(config);
        var illegalStateException = assertThrows(IllegalStateException.class, () -> {
            try (var ignored = new KafkaProxy(configParser, parsedConfiguration, Features.defaultFeatures())) {
                fail("The proxy started, but a failure was expected.");
            }
        });
        assertThat(illegalStateException).hasStackTraceContaining(expectedMessage);
    }

    public static Stream<Arguments> parametersNonNullable() {
        return Stream.of(Arguments.of(null, Mockito.mock(Configuration.class), Features.defaultFeatures()),
                Arguments.of(Mockito.mock(PluginFactoryRegistry.class), null, Features.defaultFeatures()),
                Arguments.of(Mockito.mock(PluginFactoryRegistry.class), Mockito.mock(Configuration.class), null));
    }

    @ParameterizedTest
    @MethodSource
    void parametersNonNullable(@NonNull PluginFactoryRegistry pfr, @NonNull Configuration config, @NonNull Features features) {
        assertThatThrownBy(() -> {
            new KafkaProxy(pfr, config, features);
        }).isInstanceOf(NullPointerException.class);
    }

    @Test
    void invalidConfigurationForFeatures() {
        Optional<Map<String, Object>> a = Optional.of(Map.of("a", "b"));
        Configuration configuration = new Configuration(null, null, List.of(), null, false, a);
        Features features = Features.defaultFeatures();
        assertThatThrownBy(() -> {
            KafkaProxy.validate(configuration, features);
        }).isInstanceOf(IllegalConfigurationException.class)
                .hasMessage("invalid configuration: test-only configuration for proxy present, but loading test-only configuration not enabled");
    }

}
