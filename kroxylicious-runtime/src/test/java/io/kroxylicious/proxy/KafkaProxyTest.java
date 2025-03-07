/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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

class KafkaProxyTest {

    @Test
    void shouldFailToStartIfRequireFilterConfigIsMissing() throws Exception {
        var config = """
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: cluster1:9192
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

    static Stream<Arguments> detectsConflictingPorts() {
        return Stream.of(Arguments.of("bootstrap port conflict", """
                virtualClusters:
                  - name: demo1
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9192
                  - name: demo2
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9192 # Conflict
                """,
                "exclusive TCP bind of <any>:9192 for gateway 'default' of virtual cluster 'demo1' conflicts with exclusive TCP bind of <any>:9192 for gateway 'default' of virtual cluster 'demo2': exclusive port collision"),
                Arguments.of("broker port conflict", """
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            gateways:
                            - name: default
                              portIdentifiesNode:
                                bootstrapAddress: localhost:9192
                                nodeStartPort: 9193
                          - name: demo2
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            gateways:
                            - name: default
                              portIdentifiesNode:
                                bootstrapAddress: localhost:8192
                                nodeStartPort: 9193 # Conflict
                        """,
                        "exclusive TCP bind of <any>:9193 for gateway 'default' of virtual cluster 'demo1' conflicts with exclusive TCP bind of <any>:9193 for gateway 'default' of virtual cluster 'demo2': exclusive port collision"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void detectsConflictingPorts(String name, String config, String expectedMessage) throws Exception {
        ConfigParser configParser = new ConfigParser();
        try (var kafkaProxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            assertThatThrownBy(kafkaProxy::startup).hasMessageContaining(expectedMessage);
        }
    }

    static Stream<Arguments> missingTls() {
        return Stream.of(Arguments.of("tls mismatch", """
                virtualClusters:
                  - name: demo1
                    gateways:
                    - name: default
                      sniHostIdentifiesNode:
                        bootstrapAddress: localhost:8192
                        advertisedBrokerAddressPattern: broker-$(nodeId)
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                """,
                "When using 'sniHostIdentifiesNode', 'tls' must be provided (virtual cluster listener default)"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void missingTls(String name, String config, String expectedMessage) {
        ConfigParser configParser = new ConfigParser();

        assertThatThrownBy(() -> configParser.parseConfiguration(config))
                .hasStackTraceContaining(expectedMessage);
    }

    public static Stream<Arguments> parametersNonNullable() {
        return Stream.of(Arguments.of(null, Mockito.mock(Configuration.class), Features.defaultFeatures()),
                Arguments.of(Mockito.mock(PluginFactoryRegistry.class), null, Features.defaultFeatures()),
                Arguments.of(Mockito.mock(PluginFactoryRegistry.class), Mockito.mock(Configuration.class), null));
    }

    @ParameterizedTest
    @MethodSource
    @SuppressWarnings("resource")
    void parametersNonNullable(@NonNull PluginFactoryRegistry pfr, @NonNull Configuration config, @NonNull Features features) {
        assertThatThrownBy(() -> new KafkaProxy(pfr, config, features)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void invalidConfigurationForFeatures() {
        Optional<Map<String, Object>> a = Optional.of(Map.of("a", "b"));
        Configuration configuration = new Configuration(null, List.of(), null, List.of(), null, false, a);
        Features features = Features.defaultFeatures();
        assertThatThrownBy(() -> {
            KafkaProxy.validate(configuration, features);
        }).isInstanceOf(IllegalConfigurationException.class)
                .hasMessage("invalid configuration: test-only configuration for proxy present, but loading test-only configuration not enabled");
    }

    @Test
    void supportsLivezEndpoint() throws Exception {
        var config = """
                   adminHttp:
                    port: 9190
                   virtualClusters:
                     - name: demo1
                       targetCluster:
                         bootstrapServers: kafka.example:1234
                       gateways:
                       - name: default
                         portIdentifiesNode:
                           bootstrapAddress: localhost:9192
                   filters: []
                """;
        var configParser = new ConfigParser();
        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures())) {
            proxy.startup();

            var client = HttpClient.newHttpClient();
            var uri = URI.create("http://localhost:9190/livez");
            var response = client.send(HttpRequest.newBuilder(uri).GET().build(), HttpResponse.BodyHandlers.discarding());
            assertThat(response.statusCode()).isEqualTo(200);
        }
    }
}
