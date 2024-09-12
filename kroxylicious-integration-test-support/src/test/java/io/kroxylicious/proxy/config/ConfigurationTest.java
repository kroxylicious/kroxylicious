/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.zjsonpatch.JsonDiff;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigurationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private final ConfigParser configParser = new ConfigParser();

    public static Stream<Arguments> fluentApiConfigYamlFidelity() {
        return Stream.of(
                Arguments.of(
                        "Top level",
                        new ConfigurationBuilder().withUseIoUring(true).build(),
                        """
                                useIoUring: true"""
                ),
                Arguments.of(
                        "With filter",
                        new ConfigurationBuilder()
                                                  .addToFilters(
                                                          new FilterDefinitionBuilder(ExampleFilterFactory.class.getSimpleName())
                                                                                                                                 .withConfig(
                                                                                                                                         "examplePlugin",
                                                                                                                                         "ExamplePluginInstance",
                                                                                                                                         "examplePluginConfig",
                                                                                                                                         Map.of(
                                                                                                                                                 "pluginKey",
                                                                                                                                                 "pluginValue"
                                                                                                                                         )
                                                                                                                                 )
                                                                                                                                 .build()
                                                  )
                                                  .build(),
                        """
                                    filters:
                                    - type: ExampleFilterFactory
                                      config:
                                        examplePlugin: ExamplePluginInstance
                                        examplePluginConfig:
                                          pluginKey: pluginValue
                                """
                ),
                Arguments.of(
                        "With Virtual Cluster",
                        new ConfigurationBuilder()
                                                  .addToVirtualClusters(
                                                          "demo",
                                                          new VirtualClusterBuilder()
                                                                                     .withNewTargetCluster()
                                                                                     .withBootstrapServers("kafka.example:1234")
                                                                                     .endTargetCluster()
                                                                                     .withClusterNetworkAddressConfigProvider(
                                                                                             new ClusterNetworkAddressConfigProviderDefinitionBuilder(
                                                                                                     "SniRoutingClusterNetworkAddressConfigProvider"
                                                                                             )
                                                                                              .withConfig(
                                                                                                      "bootstrapAddress",
                                                                                                      "cluster1:9192",
                                                                                                      "brokerAddressPattern",
                                                                                                      "broker-$(nodeId)"
                                                                                              )
                                                                                              .build()
                                                                                     )
                                                                                     .build()
                                                  )
                                                  .build(),
                        """
                                virtualClusters:
                                  demo:
                                    targetCluster:
                                      bootstrap_servers: kafka.example:1234
                                    clusterNetworkAddressConfigProvider:
                                      type: SniRoutingClusterNetworkAddressConfigProvider
                                      config:
                                        bootstrapAddress: cluster1:9192
                                        brokerAddressPattern: broker-$(nodeId)
                                """
                ),
                Arguments.of(
                        "Downstream TLS",
                        new ConfigurationBuilder()
                                                  .addToVirtualClusters(
                                                          "demo",
                                                          new VirtualClusterBuilder()
                                                                                     .withNewTargetCluster()
                                                                                     .withBootstrapServers("kafka.example:1234")
                                                                                     .endTargetCluster()
                                                                                     .withNewTls()
                                                                                     .withNewKeyPairKey()
                                                                                     .withCertificateFile("/tmp/cert")
                                                                                     .withPrivateKeyFile("/tmp/key")
                                                                                     .withNewInlinePasswordKeyProvider("keypassword")
                                                                                     .endKeyPairKey()
                                                                                     .endTls()
                                                                                     .withClusterNetworkAddressConfigProvider(
                                                                                             new ClusterNetworkAddressConfigProviderDefinitionBuilder(
                                                                                                     "SniRoutingClusterNetworkAddressConfigProvider"
                                                                                             )
                                                                                              .withConfig(
                                                                                                      "bootstrapAddress",
                                                                                                      "cluster1:9192",
                                                                                                      "brokerAddressPattern",
                                                                                                      "broker-$(nodeId)"
                                                                                              )
                                                                                              .build()
                                                                                     )
                                                                                     .build()
                                                  )
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
                                      type: SniRoutingClusterNetworkAddressConfigProvider
                                      config:
                                        bootstrapAddress: cluster1:9192
                                        brokerAddressPattern: broker-$(nodeId)
                                """
                ),
                Arguments.of(
                        "Upstream TLS - platform trust",
                        new ConfigurationBuilder()
                                                  .addToVirtualClusters(
                                                          "demo",
                                                          new VirtualClusterBuilder()
                                                                                     .withNewTargetCluster()
                                                                                     .withBootstrapServers("kafka.example:1234")
                                                                                     .withNewTls()
                                                                                     .endTls()
                                                                                     .endTargetCluster()
                                                                                     .withClusterNetworkAddressConfigProvider(
                                                                                             new ClusterNetworkAddressConfigProviderDefinitionBuilder(
                                                                                                     "SniRoutingClusterNetworkAddressConfigProvider"
                                                                                             )
                                                                                              .withConfig(
                                                                                                      "bootstrapAddress",
                                                                                                      "cluster1:9192",
                                                                                                      "brokerAddressPattern",
                                                                                                      "broker-$(nodeId)"
                                                                                              )
                                                                                              .build()
                                                                                     )
                                                                                     .build()
                                                  )
                                                  .build(),
                        """
                                virtualClusters:
                                  demo:
                                    targetCluster:
                                      bootstrap_servers: kafka.example:1234
                                      tls: {}
                                    clusterNetworkAddressConfigProvider:
                                      type: SniRoutingClusterNetworkAddressConfigProvider
                                      config:
                                        bootstrapAddress: cluster1:9192
                                        brokerAddressPattern: broker-$(nodeId)
                                """
                ),
                Arguments.of(
                        "Upstream TLS - trust from truststore",
                        new ConfigurationBuilder()
                                                  .addToVirtualClusters(
                                                          "demo",
                                                          new VirtualClusterBuilder()
                                                                                     .withNewTargetCluster()
                                                                                     .withBootstrapServers("kafka.example:1234")
                                                                                     .withNewTls()
                                                                                     .withNewTrustStoreTrust()
                                                                                     .withStoreFile("/tmp/client.jks")
                                                                                     .withStoreType("JKS")
                                                                                     .withNewInlinePasswordStoreProvider("storepassword")
                                                                                     .endTrustStoreTrust()
                                                                                     .endTls()
                                                                                     .endTargetCluster()
                                                                                     .withClusterNetworkAddressConfigProvider(
                                                                                             new ClusterNetworkAddressConfigProviderDefinitionBuilder(
                                                                                                     "SniRoutingClusterNetworkAddressConfigProvider"
                                                                                             )
                                                                                              .withConfig(
                                                                                                      "bootstrapAddress",
                                                                                                      "cluster1:9192",
                                                                                                      "brokerAddressPattern",
                                                                                                      "broker-$(nodeId)"
                                                                                              )
                                                                                              .build()
                                                                                     )
                                                                                     .build()
                                                  )
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
                                      type: SniRoutingClusterNetworkAddressConfigProvider
                                      config:
                                        bootstrapAddress: cluster1:9192
                                        brokerAddressPattern: broker-$(nodeId)
                                """
                ),
                Arguments.of(
                        "Upstream TLS - trust from truststore, password from file",
                        new ConfigurationBuilder()
                                                  .addToVirtualClusters(
                                                          "demo",
                                                          new VirtualClusterBuilder()
                                                                                     .withNewTargetCluster()
                                                                                     .withBootstrapServers("kafka.example:1234")
                                                                                     .withNewTls()
                                                                                     .withNewTrustStoreTrust()
                                                                                     .withStoreFile("/tmp/client.jks")
                                                                                     .withStoreType("JKS")
                                                                                     .withNewFilePasswordStoreProvider("/tmp/password.txt")
                                                                                     .endTrustStoreTrust()
                                                                                     .endTls()
                                                                                     .endTargetCluster()
                                                                                     .withClusterNetworkAddressConfigProvider(
                                                                                             new ClusterNetworkAddressConfigProviderDefinitionBuilder(
                                                                                                     "SniRoutingClusterNetworkAddressConfigProvider"
                                                                                             )
                                                                                              .withConfig(
                                                                                                      "bootstrapAddress",
                                                                                                      "cluster1:9192",
                                                                                                      "brokerAddressPattern",
                                                                                                      "broker-$(nodeId)"
                                                                                              )
                                                                                              .build()
                                                                                     )
                                                                                     .build()
                                                  )
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
                                              passwordFile: /tmp/password.txt
                                            storeType: JKS
                                    clusterNetworkAddressConfigProvider:
                                      type: SniRoutingClusterNetworkAddressConfigProvider
                                      config:
                                        bootstrapAddress: cluster1:9192
                                        brokerAddressPattern: broker-$(nodeId)
                                """
                ),
                Arguments.of(
                        "Upstream TLS - insecure",
                        new ConfigurationBuilder()
                                                  .addToVirtualClusters(
                                                          "demo",
                                                          new VirtualClusterBuilder()
                                                                                     .withNewTargetCluster()
                                                                                     .withBootstrapServers("kafka.example:1234")
                                                                                     .withNewTls()
                                                                                     .withNewInsecureTlsTrust(true)
                                                                                     .endTls()
                                                                                     .endTargetCluster()
                                                                                     .withClusterNetworkAddressConfigProvider(
                                                                                             new ClusterNetworkAddressConfigProviderDefinitionBuilder(
                                                                                                     "SniRoutingClusterNetworkAddressConfigProvider"
                                                                                             )
                                                                                              .withConfig(
                                                                                                      "bootstrapAddress",
                                                                                                      "cluster1:9192",
                                                                                                      "brokerAddressPattern",
                                                                                                      "broker-$(nodeId)"
                                                                                              )
                                                                                              .build()
                                                                                     )
                                                                                     .build()
                                                  )
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
                                      type: SniRoutingClusterNetworkAddressConfigProvider
                                      config:
                                        bootstrapAddress: cluster1:9192
                                        brokerAddressPattern: broker-$(nodeId)
                                """
                )

        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void fluentApiConfigYamlFidelity(String name, Configuration config, String expected) throws Exception {
        var yaml = configParser.toYaml(config);
        var actualJson = MAPPER.reader().readValue(yaml, JsonNode.class);
        var expectedJson = MAPPER.reader().readValue(expected, JsonNode.class);
        var diff = JsonDiff.asJson(actualJson, expectedJson);
        assertThat(diff).isEmpty();
    }

}
