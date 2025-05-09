/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.flipkart.zjsonpatch.JsonDiff;

import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultSniHostIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class ConfigurationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory()).registerModule(new Jdk8Module());
    private static final VirtualClusterGateway VIRTUAL_CLUSTER_GATEWAY = defaultGatewayBuilder()
            .withNewPortIdentifiesNode()
            .withBootstrapAddress(HostPort.parse("example.com:1234"))
            .endPortIdentifiesNode()
            .build();
    private static final VirtualCluster VIRTUAL_CLUSTER = new VirtualClusterBuilder()
            .withName("demo")
            .withNewTargetCluster()
            .withBootstrapServers("kafka.example:1234")
            .endTargetCluster()
            .addToGateways(VIRTUAL_CLUSTER_GATEWAY)
            .build();
    private final ConfigParser configParser = new ConfigParser();

    @Test
    void shouldRejectVirtualClusterWithNoGateways() {
        assertThatThrownBy(() -> {
            MAPPER.readValue(
                    """
                              name: cluster
                              targetCluster:
                                bootstrapServers: kafka.example:1234
                            """, VirtualCluster.class);
        }).isInstanceOf(ValueInstantiationException.class)
                .hasCauseInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("no gateways configured for virtualCluster");
    }

    @Test
    void shouldRejectVirtualClusterWithNullGateways() {
        assertThatThrownBy(() -> {
            MAPPER.readValue(
                    """
                              name: cluster
                              targetCluster:
                                bootstrapServers: kafka.example:1234
                              gateways: null
                            """, VirtualCluster.class);
        }).isInstanceOf(ValueInstantiationException.class)
                .hasCauseInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("no gateways configured for virtualCluster");
    }

    @Test
    void shouldRejectVirtualClusterNullGatewayValue() {
        assertThatThrownBy(() -> {
            MAPPER.readValue(
                    """
                              name: cluster
                              targetCluster:
                                bootstrapServers: kafka.example:1234
                              gateways: [null]
                            """, VirtualCluster.class);
        }).isInstanceOf(ValueInstantiationException.class)
                .hasCauseInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("one or more gateways were null");
    }

    @Test
    void shouldRejectVirtualClusterWithLegacyTlsButNoLegacyProvider() {
        assertThatThrownBy(() -> {
            MAPPER.readValue(
                    """
                              name: cluster
                              targetCluster:
                                bootstrapServers: kafka.example:1234
                              tls:
                                 key:
                                   certificateFile: /tmp/cert
                                   privateKeyFile: /tmp/key
                                   keyPassword:
                                     password: keypassword
                            """, VirtualCluster.class);
        }).isInstanceOf(ValueInstantiationException.class)
                .hasCauseInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Deprecated virtualCluster property 'tls' supplied, but 'clusterNetworkAddressConfigProvider' is null");
    }

    @Test
    void shouldRejectVirtualClusterWithTlsButNullLegacyProvider() {
        assertThatThrownBy(() -> {
            MAPPER.readValue(
                    """
                              name: cluster
                              targetCluster:
                                bootstrapServers: kafka.example:1234
                              tls:
                                 key:
                                   certificateFile: /tmp/cert
                                   privateKeyFile: /tmp/key
                                   keyPassword:
                                     password: keypassword
                              clusterNetworkAddressConfigProvider: null
                            """, VirtualCluster.class);
        }).isInstanceOf(ValueInstantiationException.class)
                .hasCauseInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Deprecated virtualCluster property 'tls' supplied, but 'clusterNetworkAddressConfigProvider' is null");
    }

    @Test
    void shouldRejectVirtualClusterWithLegacyProviderAndNewGateways() {
        assertThatThrownBy(() -> {
            MAPPER.readValue(
                    """
                              name: cluster
                              targetCluster:
                                bootstrapServers: kafka.example:1234
                              gateways:
                              - name: default
                                sniHostIdentifiesNode:
                                    bootstrapAddress: cluster1:9192
                                    advertisedBrokerAddressPattern: broker-$(nodeId)
                                tls:
                                  key:
                                    certificateFile: /tmp/cert
                                    privateKeyFile: /tmp/key
                              clusterNetworkAddressConfigProvider:
                                type: SniRoutingClusterNetworkAddressConfigProvider
                                config:
                                  bootstrapAddress: cluster1:9192
                                  advertisedBrokerAddressPattern: broker-$(nodeId)
                            """, VirtualCluster.class);
        }).isInstanceOf(ValueInstantiationException.class)
                .hasCauseInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("When using gateways, the virtualCluster properties 'clusterNetworkAddressConfigProvider' and 'tls' must be omitted");
    }

    @Test
    @SuppressWarnings("removal")
    void shouldAcceptDeprecatedTopLevelNetworkConfigProvider() throws IOException {
        var vc = MAPPER.readValue(
                """
                                  name: cluster
                                  targetCluster:
                                    bootstrapServers: kafka.example:1234
                                  clusterNetworkAddressConfigProvider:
                                    type: SniRoutingClusterNetworkAddressConfigProvider
                                    config:
                                      bootstrapAddress: cluster1:9192
                                      brokerAddressPattern: broker-$(nodeId)
                        """, VirtualCluster.class);

        assertThat(vc.clusterNetworkAddressConfigProvider().type())
                .isEqualTo("SniRoutingClusterNetworkAddressConfigProvider");
    }

    @Test
    @SuppressWarnings("removal")
    void shouldAcceptDeprecatedTopLevelTls() throws IOException {
        var vc = MAPPER.readValue(
                """
                                  name: cluster
                                  targetCluster:
                                    bootstrapServers: kafka.example:1234
                                  clusterNetworkAddressConfigProvider:
                                    type: SniRoutingClusterNetworkAddressConfigProvider
                                    config: {}
                                  tls:
                                     key:
                                       certificateFile: /tmp/cert
                                       privateKeyFile: /tmp/key
                        """, VirtualCluster.class);

        assertThat(vc.tls())
                .isPresent()
                .get()
                .satisfies(tls -> {
                    assertThat(tls.key())
                            .asInstanceOf(InstanceOfAssertFactories.type(KeyPair.class))
                            .satisfies(kp -> {
                                assertThat(kp.privateKeyFile()).isEqualTo("/tmp/key");
                            });
                });
    }

    static Stream<Arguments> fluentApiConfigYamlFidelity() {
        NamedFilterDefinition filter = new NamedFilterDefinitionBuilder("filter-1", ExampleFilterFactory.class.getSimpleName())
                .withConfig("examplePlugin", "ExamplePluginInstance",
                        "examplePluginConfig", Map.of("pluginKey", "pluginValue"))
                .build();
        return Stream.of(argumentSet("Top level",
                new ConfigurationBuilder().addToVirtualClusters(VIRTUAL_CLUSTER).withUseIoUring(true).build(),
                """
                        useIoUring: true
                        virtualClusters:
                          - name: demo
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            gateways:
                            - name: default
                              portIdentifiesNode:
                                bootstrapAddress: example.com:1234
                        """),
                argumentSet("With filters",
                        new ConfigurationBuilder()
                                .addToVirtualClusters(VIRTUAL_CLUSTER)
                                .addToFilters(filter.asFilterDefinition())
                                .build(),
                        """
                                    filters:
                                    - type: ExampleFilterFactory
                                      config:
                                        examplePlugin: ExamplePluginInstance
                                        examplePluginConfig:
                                          pluginKey: pluginValue
                                    virtualClusters:
                                      - name: demo
                                        targetCluster:
                                          bootstrapServers: kafka.example:1234
                                        gateways:
                                        - name: default
                                          portIdentifiesNode:
                                            bootstrapAddress: example.com:1234
                                """),
                argumentSet("With filterDefinitions",
                        new ConfigurationBuilder()
                                .addToVirtualClusters(VIRTUAL_CLUSTER)
                                .addToFilterDefinitions(filter)
                                .addToDefaultFilters(filter.name())
                                .build(),
                        """
                                    filterDefinitions:
                                    - name: filter-1
                                      type: ExampleFilterFactory
                                      config:
                                        examplePlugin: ExamplePluginInstance
                                        examplePluginConfig:
                                          pluginKey: pluginValue
                                    defaultFilters:
                                      - filter-1
                                    virtualClusters:
                                      - name: demo
                                        targetCluster:
                                          bootstrapServers: kafka.example:1234
                                        gateways:
                                        - name: default
                                          portIdentifiesNode:
                                            bootstrapAddress: example.com:1234
                                """),
                argumentSet("With Virtual Cluster - single gateway",
                        new ConfigurationBuilder()
                                .addToVirtualClusters(new VirtualClusterBuilder()
                                        .withName("demo")
                                        .withNewTargetCluster()
                                        .withBootstrapServers("kafka.example:1234")
                                        .endTargetCluster()
                                        .addToGateways(KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder(HostPort.parse("cluster1:9192")).build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: kafka.example:1234
                                    gateways:
                                    - name: default
                                      portIdentifiesNode:
                                        bootstrapAddress: cluster1:9192
                                """),
                argumentSet("With Virtual Cluster - multiple gateways",
                        new ConfigurationBuilder()
                                .addToVirtualClusters(new VirtualClusterBuilder()
                                        .withName("demo")
                                        .withNewTargetCluster()
                                        .withBootstrapServers("kafka.example:1234")
                                        .endTargetCluster()
                                        .addToGateways(new VirtualClusterGatewayBuilder()
                                                .withName("gateway1")
                                                .withNewPortIdentifiesNode()
                                                .withBootstrapAddress(HostPort.parse("localhost:9192"))
                                                .endPortIdentifiesNode()
                                                .build())
                                        .addToGateways(new VirtualClusterGatewayBuilder()
                                                .withName("gateway2")
                                                .withNewPortIdentifiesNode()
                                                .withBootstrapAddress(HostPort.parse("localhost:9292"))
                                                .endPortIdentifiesNode()
                                                .build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: kafka.example:1234
                                    gateways:
                                    - name: gateway1
                                      portIdentifiesNode:
                                          bootstrapAddress: localhost:9192
                                    - name: gateway2
                                      portIdentifiesNode:
                                          bootstrapAddress: localhost:9292
                                """),
                argumentSet("Downstream TLS - default client auth",
                        new ConfigurationBuilder()
                                .addToVirtualClusters(new VirtualClusterBuilder()
                                        .withName("demo")
                                        .withNewTargetCluster()
                                        .withBootstrapServers("kafka.example:1234")
                                        .endTargetCluster()
                                        .addToGateways(defaultSniHostIdentifiesNodeGatewayBuilder("cluster1:9192", "broker-$(nodeId)")
                                                .withNewTls()
                                                .withNewKeyPairKey()
                                                .withCertificateFile("/tmp/cert")
                                                .withPrivateKeyFile("/tmp/key")
                                                .withNewInlinePasswordKeyProvider("keypassword")
                                                .endKeyPairKey()
                                                .endTls()
                                                .build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: kafka.example:1234
                                    gateways:
                                    - name: default
                                      sniHostIdentifiesNode:
                                         bootstrapAddress: cluster1:9192
                                         advertisedBrokerAddressPattern: broker-$(nodeId)
                                      tls:
                                         key:
                                           certificateFile: /tmp/cert
                                           privateKeyFile: /tmp/key
                                           keyPassword:
                                             password: keypassword
                                """),
                argumentSet("Downstream TLS - required client auth",
                        new ConfigurationBuilder()
                                .addToVirtualClusters(new VirtualClusterBuilder()
                                        .withName("demo")
                                        .withNewTargetCluster()
                                        .withBootstrapServers("kafka.example:1234")
                                        .endTargetCluster()
                                        .addToGateways(defaultSniHostIdentifiesNodeGatewayBuilder("cluster1:9192", "broker-$(nodeId)")
                                                .withNewTls()
                                                .withNewKeyPairKey()
                                                .withCertificateFile("/tmp/cert")
                                                .withPrivateKeyFile("/tmp/key")
                                                .withNewInlinePasswordKeyProvider("keypassword")
                                                .endKeyPairKey()
                                                .withNewTrustStoreTrust()
                                                .withNewServerOptionsTrust()
                                                .withClientAuth(TlsClientAuth.REQUIRED)
                                                .endServerOptionsTrust()
                                                .withStoreFile("/tmp/trust")
                                                .endTrustStoreTrust().endTls()
                                                .build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: kafka.example:1234
                                    gateways:
                                    - name: default
                                      sniHostIdentifiesNode:
                                        bootstrapAddress: cluster1:9192
                                        advertisedBrokerAddressPattern: broker-$(nodeId)
                                      tls:
                                        key:
                                          certificateFile: /tmp/cert
                                          privateKeyFile: /tmp/key
                                          keyPassword:
                                            password: keypassword
                                        trust:
                                          storeFile: /tmp/trust
                                          trustOptions:
                                            clientAuth: REQUIRED
                                """),
                argumentSet("Upstream TLS - platform trust",
                        new ConfigurationBuilder()
                                .addToVirtualClusters(new VirtualClusterBuilder()
                                        .withName("demo")
                                        .withNewTargetCluster()
                                        .withBootstrapServers("kafka.example:1234")
                                        .withNewTls()
                                        .endTls()
                                        .endTargetCluster()
                                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("cluster1:9192").build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: kafka.example:1234
                                      tls: {}
                                    gateways:
                                    - name: default
                                      portIdentifiesNode:
                                        bootstrapAddress: cluster1:9192
                                """),
                argumentSet("Upstream TLS - trust from truststore",
                        new ConfigurationBuilder()
                                .addToVirtualClusters(new VirtualClusterBuilder()
                                        .withName("demo")
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
                                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("cluster1:9192").build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: kafka.example:1234
                                      tls:
                                         trust:
                                            storeFile: /tmp/client.jks
                                            storePassword:
                                              password: storepassword
                                            storeType: JKS
                                    gateways:
                                    - name: default
                                      portIdentifiesNode:
                                        bootstrapAddress: cluster1:9192
                                """),
                argumentSet("Upstream TLS - trust from truststore, password from file",
                        new ConfigurationBuilder()
                                .addToVirtualClusters(new VirtualClusterBuilder()
                                        .withName("demo")
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
                                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("cluster1:9192").build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: kafka.example:1234
                                      tls:
                                         trust:
                                            storeFile: /tmp/client.jks
                                            storePassword:
                                              passwordFile: /tmp/password.txt
                                            storeType: JKS
                                    gateways:
                                    - name: default
                                      portIdentifiesNode:
                                          bootstrapAddress: cluster1:9192
                                """),
                argumentSet("Upstream TLS - insecure",
                        new ConfigurationBuilder()
                                .addToVirtualClusters(new VirtualClusterBuilder()
                                        .withName("demo")
                                        .withNewTargetCluster()
                                        .withBootstrapServers("kafka.example:1234")
                                        .withNewTls()
                                        .withNewInsecureTlsTrust(true)
                                        .endTls()
                                        .endTargetCluster()
                                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("cluster1:9192").build())
                                        .build())
                                .build(),
                        """
                                virtualClusters:
                                  - name: demo
                                    targetCluster:
                                      bootstrapServers: kafka.example:1234
                                      tls:
                                         trust:
                                            insecure: true
                                    gateways:
                                    - name: default
                                      portIdentifiesNode:
                                        bootstrapAddress: cluster1:9192
                                """)

        );
    }

    @ParameterizedTest
    @MethodSource
    void fluentApiConfigYamlFidelity(Configuration config, String expected) throws Exception {
        var yaml = configParser.toYaml(config);
        var actualJson = MAPPER.reader().readValue(yaml, JsonNode.class);
        var expectedJson = MAPPER.reader().readValue(expected, JsonNode.class);
        var diff = JsonDiff.asJson(actualJson, expectedJson);
        assertThat(diff).isEmpty();
    }

    @Test
    void shouldGenerateUniqueNames() {
        List<FilterDefinition> filters = List.of(
                new FilterDefinition("Bar", "1"),
                new FilterDefinition("Foo", "2"),
                new FilterDefinition("Bar", "3"));
        assertThat(Configuration.toNamedFilterDefinitions(filters)).isEqualTo(List.of(
                new NamedFilterDefinition("Bar-0", "Bar", "1"),
                new NamedFilterDefinition("Foo", "Foo", "2"),
                new NamedFilterDefinition("Bar-1", "Bar", "3")));
    }

    @Test
    @SuppressWarnings({ "java:S5738", "removal" })
    void shouldRejectBothFiltersAndFilterDefinitions() {
        List<NamedFilterDefinition> filterDefinitions = List.of(new NamedFilterDefinition("foo", "", ""));
        List<FilterDefinition> filters = List.of(new FilterDefinition("", ""));
        Optional<Map<String, Object>> development = Optional.empty();
        var virtualCluster = List.of(VIRTUAL_CLUSTER);
        assertThatThrownBy(() -> new Configuration(null,
                filterDefinitions,
                null,
                virtualCluster,
                filters,
                null,
                false,
                development))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessage("'filters' and 'filterDefinitions' can't both be set");
    }

    @Test
    @SuppressWarnings({ "java:S5738", "removal" })
    void shouldRejectFilterDefinitionsWithSameName() {
        List<NamedFilterDefinition> filterDefinitions = List.of(
                new NamedFilterDefinition("foo", "", ""),
                new NamedFilterDefinition("foo", "", ""));
        Optional<Map<String, Object>> development = Optional.empty();
        var virtualCluster = List.of(VIRTUAL_CLUSTER);
        assertThatThrownBy(() -> new Configuration(null,
                filterDefinitions,
                null,
                virtualCluster,
                null,
                null,
                false,
                development))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessage("'filterDefinitions' contains multiple items with the same names: [foo]");
    }

    @Test
    void shouldRejectMissingDefaultFilter() {
        Optional<Map<String, Object>> development = Optional.empty();
        List<NamedFilterDefinition> filterDefinitions = List.of();
        List<String> defaultFilters = List.of("missing");
        var virtualCluster = List.of(VIRTUAL_CLUSTER);
        assertThatThrownBy(() -> new Configuration(null, filterDefinitions,
                defaultFilters,
                virtualCluster,
                null, false,
                development))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessage("'defaultFilters' references filters not defined in 'filterDefinitions': [missing]");
    }

    @Test
    void shouldRejectMissingClusterFilter() {
        Optional<Map<String, Object>> development = Optional.empty();
        List<NamedFilterDefinition> filterDefinitions = List.of();
        List<VirtualClusterGateway> defaultGateway = List.of(VIRTUAL_CLUSTER_GATEWAY);
        TargetCluster targetCluster = new TargetCluster("unused:9082", Optional.empty());
        List<VirtualCluster> virtualClusters = List
                .of(new VirtualCluster("vc1", targetCluster, null, Optional.empty(), defaultGateway, false, false, List.of("missing")));
        assertThatThrownBy(() -> new Configuration(
                null, filterDefinitions,
                null,
                virtualClusters,
                null, false,
                development))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessage("'virtualClusters.vc1.filters' references filters not defined in 'filterDefinitions': [missing]");
    }

    @Test
    void shouldRejectUnusedFilterDefinition() {
        Optional<Map<String, Object>> development = Optional.empty();
        List<NamedFilterDefinition> filterDefinitions = List.of(
                new NamedFilterDefinition("used1", "", ""),
                new NamedFilterDefinition("unused", "", ""),
                new NamedFilterDefinition("used2", "", "")

        );

        List<String> defaultFilters = List.of("used1");
        List<VirtualClusterGateway> defaultGateway = List.of(VIRTUAL_CLUSTER_GATEWAY);
        TargetCluster targetCluster = new TargetCluster("unused:9082", Optional.empty());
        List<VirtualCluster> virtualClusters = List.of(new VirtualCluster("vc1", targetCluster, null, Optional.empty(), defaultGateway, false, false, List.of("used2")));
        assertThatThrownBy(() -> new Configuration(null, filterDefinitions,
                defaultFilters,
                virtualClusters,
                null, false,
                development))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessage("'filterDefinitions' defines filters which are not used in 'defaultFilters' or in any virtual cluster's 'filters': [unused]");
    }

    @Test
    @SuppressWarnings({ "java:S5738", "removal" })
    void shouldRejectVirtualClusterFiltersWhenTopLevelFilters() {
        Optional<Map<String, Object>> development = Optional.empty();
        List<VirtualClusterGateway> defaultGateway = List.of(VIRTUAL_CLUSTER_GATEWAY);
        TargetCluster targetCluster = new TargetCluster("unused:9082", Optional.empty());
        List<VirtualCluster> virtualClusters = List.of(new VirtualCluster("vc1", targetCluster, null, Optional.empty(), defaultGateway, false, false, List.of()));
        assertThatThrownBy(() -> new Configuration(
                null,
                null,
                null,
                virtualClusters,
                List.<FilterDefinition> of(),
                null, false,
                development))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessage("'filters' cannot be specified on a virtual cluster when 'filters' is defined at the top level.");
    }

    @Test
    void virtualClusterModelShouldUseCorrectFilters() {
        // Given
        List<NamedFilterDefinition> filterDefinitions = List.of(
                new NamedFilterDefinition("foo", "Foo", ""),
                new NamedFilterDefinition("bar", "Bar", ""));
        VirtualCluster direct = new VirtualCluster("direct", new TargetCluster("y:9092", Optional.empty()),
                null, Optional.empty(),
                List.of(new VirtualClusterGateway("mygateway",
                        new PortIdentifiesNodeIdentificationStrategy(new HostPort("example.com", 3), null, null, null),
                        null,
                        Optional.empty())),
                false,
                false,
                List.of("foo")); // filters defined on cluster

        VirtualCluster defaulted = new VirtualCluster("defaulted", new TargetCluster("x:9092", Optional.empty()),
                null,
                Optional.empty(),
                List.of(new VirtualClusterGateway("mygateway",
                        new PortIdentifiesNodeIdentificationStrategy(new HostPort("example.com", 3), null, null, null),
                        null,
                        Optional.empty())),
                false,
                false,
                null); // filters not defined => should default to the top level

        Configuration configuration = new Configuration(
                null, filterDefinitions,
                List.of("bar"),
                List.of(direct, defaulted),
                null, false,
                Optional.empty());

        // When
        var model = configuration.virtualClusterModel(new ServiceBasedPluginFactoryRegistry());

        // Then
        var directModel = model.stream().filter(x -> x.getClusterName().equals("direct")).findFirst().get();
        var defaultModel = model.stream().filter(x -> x.getClusterName().equals("defaulted")).findFirst().get();
        assertThat(directModel.getFilters()).singleElement().extracting(NamedFilterDefinition::type).isEqualTo("Foo");
        assertThat(defaultModel.getFilters()).singleElement().extracting(NamedFilterDefinition::type).isEqualTo("Bar");
    }

    @Test
    @SuppressWarnings("removal")
    void virtualClusterModelCreatedWithDeprecatedNetworkProvider() {
        // Given
        var filterDefinitions = List.of(new NamedFilterDefinition("foo", "Foo", ""));

        var targetCluster = new TargetCluster("y:9092", Optional.empty());
        var bootstrapAddress = new HostPort("example.com", 3);
        var cluster = new VirtualCluster("myvc", targetCluster,
                new ClusterNetworkAddressConfigProviderDefinition("PortPerBrokerClusterNetworkAddressConfigProvider",
                        new PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig(bootstrapAddress, null,
                                null, null, null)),
                Optional.empty(),
                List.of(),
                false,
                false,
                null);

        Configuration configuration = new Configuration(
                null, filterDefinitions,
                List.of("foo"),
                List.of(cluster),
                null, false,
                Optional.empty());

        // When
        var models = configuration.virtualClusterModel(new ServiceBasedPluginFactoryRegistry());

        // Then
        assertThat(models)
                .singleElement()
                .satisfies(m -> {
                    assertThat(m.gateways())
                            .hasEntrySatisfying("default", l -> {
                                assertThat(l.getClusterBootstrapAddress()).isEqualTo(bootstrapAddress);
                            });
                });
    }
}
