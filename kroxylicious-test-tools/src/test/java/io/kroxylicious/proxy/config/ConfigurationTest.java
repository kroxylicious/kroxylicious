/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.zjsonpatch.JsonDiff;

import io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigurationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private final ConfigParser configParser = new ConfigParser();

    public static Stream<Arguments> yamlDeserializeSerializeFidelity() {
        return Stream.of(Arguments.of("Top level flags", """
                useIoUring: true
                """),
                Arguments.of("Virtual cluster (PortPerBroker)", """
                        virtualClusters:
                          demo1:
                            targetCluster:
                              bootstrap_servers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: PortPerBroker
                              config:
                                bootstrapAddress: cluster1:9192
                                numberOfBrokerPorts: 1
                                brokerAddressPattern: localhost:$(portNumber)
                                brokerStartPort: 9193
                        """),
                Arguments.of("Virtual cluster (SniRouting)", """
                        virtualClusters:
                          demo1:
                            targetCluster:
                              bootstrap_servers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: SniRouting
                              config:
                                bootstrapAddress: cluster1:9192
                                brokerAddressPattern: broker-$(nodeId):$(portNumber)
                        """),
                Arguments.of("Downstream/Upstream TLS", """
                        virtualClusters:
                          demo1:
                            tls:
                                key:
                                  storeFile: /tmp/foo.jks
                                  storePassword:
                                    password: password
                                  storeType: JKS
                            targetCluster:
                              bootstrap_servers: kafka.example:1234
                              tls:
                                trust:
                                 storeFile: /tmp/foo.jks
                                 storePassword:
                                   password: password
                                 storeType: JKS
                            clusterNetworkAddressConfigProvider:
                              type: SniRouting
                              config:
                                bootstrapAddress: cluster1:9192
                                brokerAddressPattern: broker-$(nodeId):$(portNumber)
                        """),
                Arguments.of("Filters", """
                        filters:
                        - type: ProduceRequestTransformation
                          config:
                            transformation: io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter$UpperCasing
                        """),
                Arguments.of("Admin", """
                        adminHttp:
                          host: 0.0.0.0
                          port: 9193
                          endpoints: {}
                        """),
                Arguments.of("Micrometer", """
                        micrometer:
                        - type: CommonTags
                          config:
                            commonTags:
                              zone: "euc-1a"
                              owner: "becky"
                        """));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    public void yamlDeserializeSerializeFidelity(String name, String config) throws Exception {

        var configuration = configParser.parseConfiguration(config);
        var roundTripped = configParser.toYaml(configuration);

        var originalJsonNode = MAPPER.reader().readValue(config, JsonNode.class);
        var roundTrippedJsonNode = MAPPER.reader().readValue(roundTripped, JsonNode.class);
        var diff = JsonDiff.asJson(originalJsonNode, roundTrippedJsonNode);
        assertThat(diff).isEmpty();

    }

    public static Stream<Arguments> fluentApiConfigYamlFidelity() {
        return Stream.of(Arguments.of("Top level",
                new ConfigurationBuilder().withUseIoUring(true).build(),
                """
                        useIoUring: true"""),

                Arguments.of("With filter",
                        new ConfigurationBuilder()
                                .addToFilters(new FilterDefinitionBuilder("ProduceRequestTransformation")
                                        .withConfig("transformation", ProduceRequestTransformationFilter.UpperCasing.class.getName()).build())
                                .build(),
                        """
                                filters:
                                - type: ProduceRequestTransformation
                                  config:
                                    transformation: "io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter$UpperCasing"
                                """),
                Arguments.of("With Virtual Cluster",
                        new ConfigurationBuilder()
                                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                                        .withNewTargetCluster()
                                        .withBootstrapServers("kafka.example:1234")
                                        .endTargetCluster()
                                        .withClusterNetworkAddressConfigProvider(
                                                new ClusterNetworkAddressConfigProviderDefinitionBuilder("SniRouting")
                                                        .withConfig("bootstrapAddress", "cluster1:9192", "brokerAddressPattern", "broker-$(nodeId):$(portNumber)")
                                                        .build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  demo:
                                    targetCluster:
                                      bootstrap_servers: kafka.example:1234
                                    clusterNetworkAddressConfigProvider:
                                      type: SniRouting
                                      config:
                                        bootstrapAddress: cluster1:9192
                                        brokerAddressPattern: broker-$(nodeId):$(portNumber)
                                """),
                Arguments.of("Downstream TLS",
                        new ConfigurationBuilder()
                                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                                        .withNewTargetCluster()
                                        .withBootstrapServers("kafka.example:1234")
                                        .endTargetCluster()
                                        .withNewTls()
                                        .withNewKeyPairKey()
                                        .withCertificateFile("/tmp/cert")
                                        .withPrivateKeyFile("/tmp/key")
                                        .withNewInlinePasswordKey("keypassword")
                                        .endKeyPairKey()
                                        .endTls()
                                        .withClusterNetworkAddressConfigProvider(
                                                new ClusterNetworkAddressConfigProviderDefinitionBuilder("SniRouting")
                                                        .withConfig("bootstrapAddress", "cluster1:9192", "brokerAddressPattern", "broker-$(nodeId):$(portNumber)")
                                                        .build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  demo:
                                    targetCluster:
                                      bootstrap_servers: kafka.example:1234
                                    tls:
                                       key:
                                         certificateFile: /tmp/cert
                                         privateKeyFile: /tmp/key
                                         keyPassword:
                                           password: keypassword
                                    clusterNetworkAddressConfigProvider:
                                      type: SniRouting
                                      config:
                                        bootstrapAddress: cluster1:9192
                                        brokerAddressPattern: broker-$(nodeId):$(portNumber)
                                """),
                Arguments.of("Upstream TLS - platform trust",
                        new ConfigurationBuilder()
                                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                                        .withNewTargetCluster()
                                        .withBootstrapServers("kafka.example:1234")
                                        .withNewTls()
                                        .endTls()
                                        .endTargetCluster()
                                        .withClusterNetworkAddressConfigProvider(
                                                new ClusterNetworkAddressConfigProviderDefinitionBuilder("SniRouting")
                                                        .withConfig("bootstrapAddress", "cluster1:9192", "brokerAddressPattern", "broker-$(nodeId):$(portNumber)")
                                                        .build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  demo:
                                    targetCluster:
                                      bootstrap_servers: kafka.example:1234
                                      tls: {}
                                    clusterNetworkAddressConfigProvider:
                                      type: SniRouting
                                      config:
                                        bootstrapAddress: cluster1:9192
                                        brokerAddressPattern: broker-$(nodeId):$(portNumber)
                                """),
                Arguments.of("Upstream TLS - trust from truststore",
                        new ConfigurationBuilder()
                                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                                        .withNewTargetCluster()
                                        .withBootstrapServers("kafka.example:1234")
                                        .withNewTls()
                                        .withNewTrustStoreTrust()
                                        .withStoreFile("/tmp/client.jks")
                                        .withStoreType("JKS")
                                        .withNewInlinePasswordStore("storepassword")
                                        .endTrustStoreTrust()
                                        .endTls()
                                        .endTargetCluster()
                                        .withClusterNetworkAddressConfigProvider(
                                                new ClusterNetworkAddressConfigProviderDefinitionBuilder("SniRouting")
                                                        .withConfig("bootstrapAddress", "cluster1:9192", "brokerAddressPattern", "broker-$(nodeId):$(portNumber)")
                                                        .build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  demo:
                                    targetCluster:
                                      bootstrap_servers: kafka.example:1234
                                      tls:
                                         trust:
                                            storeFile: /tmp/client.jks
                                            storePassword:
                                              password: storepassword
                                            storeType: JKS
                                    clusterNetworkAddressConfigProvider:
                                      type: SniRouting
                                      config:
                                        bootstrapAddress: cluster1:9192
                                        brokerAddressPattern: broker-$(nodeId):$(portNumber)
                                """),
                Arguments.of("Upstream TLS - insecure",
                        new ConfigurationBuilder()
                                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                                        .withNewTargetCluster()
                                        .withBootstrapServers("kafka.example:1234")
                                        .withNewTls()
                                        .withNewInsecureTlsTrust(true)
                                        .endTls()
                                        .endTargetCluster()
                                        .withClusterNetworkAddressConfigProvider(
                                                new ClusterNetworkAddressConfigProviderDefinitionBuilder("SniRouting")
                                                        .withConfig("bootstrapAddress", "cluster1:9192", "brokerAddressPattern", "broker-$(nodeId):$(portNumber)")
                                                        .build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  demo:
                                    targetCluster:
                                      bootstrap_servers: kafka.example:1234
                                      tls:
                                         trust:
                                            insecure: true
                                    clusterNetworkAddressConfigProvider:
                                      type: SniRouting
                                      config:
                                        bootstrapAddress: cluster1:9192
                                        brokerAddressPattern: broker-$(nodeId):$(portNumber)
                                """)

        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    public void fluentApiConfigYamlFidelity(String name, Configuration config, String expected) throws Exception {
        var yaml = configParser.toYaml(config);
        var actualJson = MAPPER.reader().readValue(yaml, JsonNode.class);
        var expectedJson = MAPPER.reader().readValue(expected, JsonNode.class);
        var diff = JsonDiff.asJson(actualJson, expectedJson);
        assertThat(diff).isEmpty();

    }
}
