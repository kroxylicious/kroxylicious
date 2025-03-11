/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.zjsonpatch.JsonDiff;

import io.kroxylicious.proxy.config.tls.TlsTestConstants;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.internal.filter.ConstructorInjectionConfig;
import io.kroxylicious.proxy.internal.filter.ExamplePluginFactory;
import io.kroxylicious.proxy.internal.filter.FactoryMethodConfig;
import io.kroxylicious.proxy.internal.filter.FieldInjectionConfig;
import io.kroxylicious.proxy.internal.filter.NestedPluginConfigFactory;
import io.kroxylicious.proxy.internal.filter.RecordConfig;
import io.kroxylicious.proxy.internal.filter.SetterInjectionConfig;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigParserTest {
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    // Given
    private final ConfigParser configParser = new ConfigParser();

    static Stream<Arguments> yamlDeserializeSerializeFidelity() {
        return Stream.of(Arguments.argumentSet("Top level flags", """
                useIoUring: true
                """),
                Arguments.argumentSet("Virtual cluster (portIdentifiesNode - minimal)", """
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            gateways:
                            - name: default
                              portIdentifiesNode:
                                  bootstrapAddress: cluster1:9192
                        """),
                Arguments.argumentSet("Virtual cluster (portIdentifiesNode with start port)", """
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            gateways:
                            - name: default
                              portIdentifiesNode:
                                  bootstrapAddress: cluster1:9192
                                  advertisedBrokerAddressPattern: localhost
                                  nodeStartPort: 9193
                        """),
                Arguments.argumentSet("Virtual cluster (portIdentifiesNode with ranges)", """
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            gateways:
                            - name: default
                              portIdentifiesNode:
                                bootstrapAddress: cluster1:9192
                                nodeIdRanges:
                                - name: range1
                                  start: 0
                                  end: 3
                                - name: range2
                                  start: 5
                                  end: 9
                        """),
                Arguments.argumentSet("Virtual cluster (portIdentifiesNode with range and start port)", """
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            gateways:
                            - name: default
                              portIdentifiesNode:
                                bootstrapAddress: cluster1:9192
                                advertisedBrokerAddressPattern: localhost
                                nodeStartPort: 9193
                                nodeIdRanges:
                                - name: brokers
                                  start: 0
                                  end: 3
                        """),
                Arguments.argumentSet("Virtual cluster (sniHostIdentifiesNode)", """
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            gateways:
                            - name: default
                              sniHostIdentifiesNode:
                                bootstrapAddress: cluster1:9192
                                advertisedBrokerAddressPattern: broker-$(nodeId)
                              tls:
                                  key:
                                    storeFile: /tmp/foo.jks
                                    storePassword:
                                      password: password

                        """),
                Arguments.argumentSet("Downstream/Upstream TLS with inline passwords", """
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                              tls:
                                trust:
                                 storeFile: /tmp/foo.jks
                                 storePassword:
                                   password: password
                                 storeType: JKS
                            gateways:
                            - name: default
                              sniHostIdentifiesNode:
                                bootstrapAddress: cluster1:9192
                                advertisedBrokerAddressPattern: broker-$(nodeId)
                              tls:
                                  key:
                                    storeFile: /tmp/foo.jks
                                    storePassword:
                                      password: password
                                    storeType: JKS
                        """),
                Arguments.argumentSet("Downstream/Upstream TLS with password files", """
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                              tls:
                                trust:
                                 storeFile: /tmp/foo.jks
                                 storePassword:
                                    passwordFile: /tmp/password.txt
                                 storeType: JKS
                            gateways:
                            - name: default
                              sniHostIdentifiesNode:
                                bootstrapAddress: cluster1:9192
                                advertisedBrokerAddressPattern: broker-$(nodeId)
                              tls:
                                  key:
                                    storeFile: /tmp/foo.jks
                                    storePassword:
                                      passwordFile: /tmp/password.txt
                                    storeType: JKS
                        """),
                Arguments.argumentSet("Virtual cluster (PortPerBroker - deprecated)", """
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: PortPerBrokerClusterNetworkAddressConfigProvider
                              config:
                                bootstrapAddress: cluster1:9192
                                numberOfBrokerPorts: 1
                                brokerAddressPattern: localhost
                                brokerStartPort: 9193
                            tls:
                              key:
                                storeFile: /tmp/foo.jks
                                storePassword:
                                  passwordFile: /tmp/password.txt
                                storeType: JKS
                        """),
                Arguments.argumentSet("Virtual cluster (RangeAwarePortPerNode - deprecated)", """
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: RangeAwarePortPerNodeClusterNetworkAddressConfigProvider
                              config:
                                bootstrapAddress: cluster1:9192
                                nodeAddressPattern: cluster1
                                nodeStartPort: 9193
                                nodeIdRanges:
                                - name: myrange
                                  range:
                                    startInclusive: 0
                                    endExclusive: 1
                        """),
                Arguments.argumentSet("Virtual cluster (SniRouting - deprecated)", """
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: SniRoutingClusterNetworkAddressConfigProvider
                              config:
                                bootstrapAddress: cluster1:9192
                                brokerAddressPattern: broker$(nodeId)
                        """),
                Arguments.argumentSet("Filters", """
                        filters:
                        - type: TestFilterFactory
                        """),
                Arguments.argumentSet("Admin", """
                        adminHttp:
                          host: 0.0.0.0
                          port: 9193
                          endpoints: {}
                        """),
                Arguments.argumentSet("Micrometer", """
                        micrometer:
                        - type: CommonTagsHook
                          config:
                            commonTags:
                              zone: "euc-1a"
                              owner: "becky"
                        """),
                Arguments.argumentSet("AdminHttp", """
                        adminHttp:
                          host: kroxy
                          port: 9093
                          endpoints:
                            prometheus: {}
                        """));
    }

    @ParameterizedTest
    @MethodSource
    void yamlDeserializeSerializeFidelity(String config) throws Exception {
        var configuration = configParser.parseConfiguration(config);
        var roundTripped = configParser.toYaml(configuration);

        var originalJsonNode = MAPPER.reader().readValue(config, JsonNode.class);
        var roundTrippedJsonNode = MAPPER.reader().readValue(roundTripped, JsonNode.class);
        var diff = JsonDiff.asJson(originalJsonNode, roundTrippedJsonNode);
        assertThat(diff).isEmpty();
    }

    @Test
    @SuppressWarnings("removal")
    void testDeserializeFromYaml() {
        Configuration configuration = configParser.parseConfiguration(this.getClass().getClassLoader().getResourceAsStream("config.yaml"));
        assertThat(configuration.isUseIoUring()).isTrue();
        assertThat(configuration.adminHttpConfig())
                .isNotNull()
                .satisfies(ahc -> {
                    assertThat(ahc.host()).isEqualTo("kroxy");
                    assertThat(ahc.port()).isEqualTo(9093);
                    assertThat(ahc.endpoints().maybePrometheus()).isPresent();
                });

        assertThat(configuration.virtualClusters())
                .singleElement()
                .satisfies(cluster -> {
                    assertThat(cluster.name()).isEqualTo("demo");
                    assertThat(cluster.logFrames()).isTrue();
                    assertThat(cluster.logNetwork()).isTrue();
                    assertThat(cluster.targetCluster()).isNotNull();
                    assertThat(cluster.targetCluster().bootstrapServers()).isEqualTo("localhost:9092");

                    assertThat(cluster.gateways())
                            .singleElement()
                            .satisfies(vcl -> {
                                assertThat(vcl.name()).isEqualTo("mygateway");
                                assertThat(vcl.clusterNetworkAddressConfigProvider())
                                        .extracting(ClusterNetworkAddressConfigProviderDefinition::config)
                                        .asInstanceOf(InstanceOfAssertFactories.type(RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig.class))
                                        .satisfies(c -> assertThat(c.getBootstrapAddress())
                                                .isEqualTo(HostPort.parse("localhost:9192")));
                            });
                });
    }

    @Test
    void testConfigParserBadJson() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> configParser.parseConfiguration("}"));
        assertThat(exception.getMessage()).contains("Couldn't parse configuration");
    }

    @SuppressWarnings("resource")
    @Test
    void testConfigParserIoException() {
        InputStream mockInputStream = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("I am the worst byte stream");
            }
        };
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> configParser.parseConfiguration(mockInputStream));
        assertThat(exception.getMessage()).contains("Couldn't parse configuration");
        assertThat(exception.getCause()).isInstanceOf(IOException.class);
        assertThat(exception.getCause()).hasMessageContaining("I am the worst byte stream");
    }

    @Test
    void shouldConfigureClusterNameFromNodeName() {
        final Configuration configurationModel = configParser.parseConfiguration("""
                virtualClusters:
                  - name: myAwesomeCluster
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: cluster1:9192
                """);
        // When
        var actualValidClusters = configurationModel.virtualClusterModel(new ServiceBasedPluginFactoryRegistry());

        // Then
        assertThat(actualValidClusters).singleElement().extracting("clusterName").isEqualTo("myAwesomeCluster");
    }

    @Test
    void shouldSupportDeprecatedVirtualClusterMap() {
        final Configuration configurationModel = configParser.parseConfiguration("""
                virtualClusters:
                  mycluster:
                    targetCluster:
                      bootstrapServers: kafka1.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: cluster1:9192
                """);
        // When
        var actualValidClusters = configurationModel.virtualClusterModel(new ServiceBasedPluginFactoryRegistry());

        // Then
        assertThat(actualValidClusters)
                .singleElement()
                .satisfies(vc -> {
                    assertThat(vc.getClusterName()).isEqualTo("mycluster");
                    assertThat(vc.targetCluster())
                            .extracting(TargetCluster::bootstrapServers)
                            .isEqualTo("kafka1.example:1234");
                });
    }

    @Test
    void shouldSupportDeprecatedVirtualClusterMapWithValueProvidingNameToo() {
        final Configuration configurationModel = configParser.parseConfiguration("""
                virtualClusters:
                  mycluster:
                    name: mycluster # matches key
                    targetCluster:
                      bootstrapServers: kafka1.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: cluster1:9192
                """);
        // When
        var actualValidClusters = configurationModel.virtualClusterModel(new ServiceBasedPluginFactoryRegistry());

        // Then
        assertThat(actualValidClusters)
                .extracting(VirtualClusterModel::getClusterName)
                .singleElement()
                .isEqualTo("mycluster");
    }

    @Test
    void shouldDetectInconsistentClusterNameInDeprecatedVirtualClusterMap() {
        // When/Then
        assertThatThrownBy(() -> {
            configParser.parseConfiguration("""
                    virtualClusters:
                      mycluster:
                        name: mycluster1
                        targetCluster:
                          bootstrapServers: kafka1.example:1234
                        gateways:
                        - name: default
                          portIdentifiesNode:
                            bootstrapAddress: cluster1:9192
                    """);

        }).hasRootCauseInstanceOf(IllegalConfigurationException.class)
                .hasRootCauseMessage(
                        "Inconsistent virtual cluster configuration. Configuration property 'virtualClusters' refers to a map, but the key name 'mycluster' is different to the value of the 'name' field 'mycluster1' in the value.");
    }

    @Test
    void shouldDetectDuplicateClusterNodeNames() {
        // Given
        assertThatThrownBy(() ->
        // When
        configParser.parseConfiguration("""
                virtualClusters:
                  - name: demo1
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: cluster1:9192
                  - name: demo1
                    targetCluster:
                      bootstrapServers: magic-kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: cluster1:9192
                """))
                // Then
                .isInstanceOf(IllegalArgumentException.class)
                .hasCauseInstanceOf(JsonMappingException.class) // Debatable to enforce the wrapped JsonMappingException
                .cause()
                .hasMessageContaining("Virtual cluster must be unique. The following virtual cluster names are duplicated: [demo1]");
    }

    @Test
    void shouldDetectMissingClusterName() {
        // Given
        assertThatThrownBy(() ->
        // When
        configParser.parseConfiguration("""
                virtualClusters:
                  - targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: cluster1:9192
                """))
                // Then
                .isInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("Missing required creator property 'name'");
    }

    @Test
    void shouldDetectMissingTargetCluster() {
        // Given
        assertThatThrownBy(() ->
        // When
        configParser.parseConfiguration("""
                virtualClusters:
                  - name: demo
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: cluster1:9192
                """))
                // Then
                .isInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("Missing required creator property 'targetCluster'");
    }

    @Test
    void testNestedPlugins() {
        ConfigParser cp = new ConfigParser();
        var config = cp.parseConfiguration("""
                filters:
                - type: NestedPluginConfigFactory
                  config:
                    examplePlugin: ExamplePluginInstance
                """);
        assertThat(config.filters()).hasSize(1);

        FilterDefinition fd = config.filters().get(0);
        assertThat(fd.type()).isEqualTo("NestedPluginConfigFactory");
        FilterFactory<?, ?> ff = cp.pluginFactory(FilterFactory.class).pluginInstance(fd.type());
        assertThat(ff).isNotNull();
        assertThat(fd.config()).isInstanceOf(NestedPluginConfigFactory.NestedPluginConfig.class);

        var npc = (NestedPluginConfigFactory.NestedPluginConfig) fd.config();
        assertThat(npc.examplePlugin()).isEqualTo("ExamplePluginInstance");
        var ep = cp.pluginFactory(ExamplePluginFactory.class).pluginInstance(npc.examplePlugin());
        assertThat(ep).isNotNull();
    }

    @Test
    void testUnknownPlugin() {
        ConfigParser cp = new ConfigParser();
        var iae = assertThrows(IllegalArgumentException.class, () -> cp.parseConfiguration("""
                filters:
                - type: NestedPluginConfigFactory
                  config:
                    examplePlugin: NotAKnownPlugin

                """));
        var vie = assertInstanceOf(ValueInstantiationException.class, iae.getCause());
        var upie = assertInstanceOf(UnknownPluginInstanceException.class, vie.getCause());
        assertEquals("Unknown io.kroxylicious.proxy.internal.filter.ExamplePluginFactory plugin instance for "
                + "name 'NotAKnownPlugin'. "
                + "Known plugin instances are [ExamplePluginInstance, io.kroxylicious.proxy.internal.filter.ExamplePluginInstance]. "
                + "Plugins must be loadable by java.util.ServiceLoader and annotated with "
                + "@Plugin.",
                upie.getMessage());
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource
    void shouldWorkWithDifferentConfigCreators(String yaml, Class<?> expectedConfigType) {
        // Given
        ConfigParser cp = new ConfigParser();
        var config = cp.parseConfiguration(yaml);

        // When

        // Then
        for (FilterDefinition fd : config.filters()) {
            var pluginFactory = cp.pluginFactory((Class<FilterFactory<? super Object, ? super Object>>) (Class<?>) FilterFactory.class);
            var filterFactory = pluginFactory.pluginInstance(fd.type());
            Class<?> configType = pluginFactory.configType(fd.type());
            assertEquals(expectedConfigType, configType);
            assertTrue(configType.isInstance(fd.config()));
            assertEquals("hello, world", filterFactory.initialize(null, fd.config()));
        }
    }

    public static Stream<Arguments> shouldWorkWithDifferentConfigCreators() {
        return Stream.of(Arguments.argumentSet("constructor injection",
                """
                        filters:
                        - type: ConstructorInjection
                          config:
                            str: hello, world
                        """,
                ConstructorInjectionConfig.class),
                Arguments.argumentSet("factory method",
                        """
                                filters:
                                - type: FactoryMethod
                                  config:
                                    str: hello, world
                                """,
                        FactoryMethodConfig.class),
                Arguments.argumentSet("field injection",
                        """
                                filters:
                                - type: FieldInjection
                                  config:
                                    str: hello, world
                                """,
                        FieldInjectionConfig.class),
                Arguments.argumentSet("record",
                        """
                                filters:
                                - type: Record
                                  config:
                                    str: hello, world
                                """,
                        RecordConfig.class),
                Arguments.argumentSet("setter injection",
                        """
                                filters:
                                - type: SetterInjection
                                  config:
                                    str: hello, world
                                """,
                        SetterInjectionConfig.class));
    }

    @Test
    void shouldThrowWhenSerializingUnserializableObject() {
        var config = new Configuration(null, List.of(new NamedFilterDefinition("foo", "", new NonSerializableConfig(""))),
                List.of("foo"), null, null, false,
                Optional.empty());

        ConfigParser cp = new ConfigParser();
        assertThatThrownBy(() -> {
            final String yaml = cp.toYaml(config);
            fail("generated YAML:\n %s", yaml);
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Failed to encode configuration as YAML");
    }

    @Test
    void shouldThrowIfMissingPluginImplName() {
        ConfigParser cp = new ConfigParser();

        var iae = assertThrows(IllegalArgumentException.class, () -> cp.parseConfiguration("""
                filters:
                - type: MissingPluginImplName
                  config:
                    id: NotAKnownPlugin
                    config:
                      foo: bar
                """));
        var vie = assertInstanceOf(ValueInstantiationException.class, iae.getCause());
        var pde = assertInstanceOf(PluginDiscoveryException.class, vie.getCause());
        assertEquals(
                "Couldn't find @PluginImplName on member referred to by @PluginImplConfig on [parameter #1, annotations: {interface io.kroxylicious.proxy.plugin.PluginImplConfig=@io.kroxylicious.proxy.plugin.PluginImplConfig(implNameProperty=\"id\")}]",
                pde.getMessage());
    }

    @Test
    void virtualClusterModelCreationWithSniHostIdentifiesNodeStrategy() {

        var bootstrapAddress = HostPort.parse("cluster1.example:9192");
        var keyStore = TlsTestConstants.getResourceLocationOnFilesystem("server.p12");
        var configurationModel = configParser.parseConfiguration("""
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: magic-kafka.example:1234
                            gateways:
                            - name: mygateway
                              tls:
                                key:
                                  storeFile: %s
                                  storePassword:
                                     password: %s
                              sniHostIdentifiesNode:
                                bootstrapAddress: "%s"
                                advertisedBrokerAddressPattern: cluster1-broker-$(nodeId).example:9192
                """.formatted(keyStore, TlsTestConstants.STOREPASS.getProvidedPassword(), bootstrapAddress));
        // When
        var models = configurationModel.virtualClusterModel(null);

        // Then
        assertThat(models)
                .singleElement()
                .satisfies(vcm -> assertThat(vcm.gateways())
                        .hasEntrySatisfying("mygateway", g -> assertThat(g.getClusterBootstrapAddress())
                                .isEqualTo(bootstrapAddress)));
    }

    @Test
    void virtualClusterModelCreationWithPortIdentifiesNodeStrategy() {

        var bootstrapAddress = HostPort.parse("cluster1.example:9192");
        var configurationModel = configParser.parseConfiguration("""
                        virtualClusters:
                          - name: demo1
                            targetCluster:
                              bootstrapServers: magic-kafka.example:1234
                            gateways:
                            - name: mygateway
                              portIdentifiesNode:
                                bootstrapAddress: "%s"
                """.formatted(bootstrapAddress));
        // When
        var models = configurationModel.virtualClusterModel(null);

        // Then
        assertThat(models)
                .singleElement()
                .satisfies(vcm -> assertThat(vcm.gateways())
                        .hasEntrySatisfying("mygateway", g -> assertThat(g.getClusterBootstrapAddress())
                                .isEqualTo(bootstrapAddress)));
    }

    private record NonSerializableConfig(String id) {
        @Override
        @JsonGetter
        public String id() {
            throw new UnsupportedOperationException("boom. haha fooled you jackson");
        }
    }
}
