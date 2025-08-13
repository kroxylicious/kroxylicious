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
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flipkart.zjsonpatch.JsonDiff;

import io.kroxylicious.proxy.bootstrap.FixedBootstrapSelectionStrategy;
import io.kroxylicious.proxy.bootstrap.RandomBootstrapSelectionStrategy;
import io.kroxylicious.proxy.config.admin.EndpointsConfiguration;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.config.admin.PrometheusMetricsConfig;
import io.kroxylicious.proxy.config.tls.TlsTestConstants;
import io.kroxylicious.proxy.filter.FilterFactory;
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
        return Stream.of(Arguments.argumentSet("With IoUring", """
                useIoUring: true
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: magic-kafka.example:1234
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
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
                Arguments.argumentSet("Filters", """
                        filterDefinitions:
                        - name: myfilter
                          type: TestFilterFactory
                        defaultFilters:
                        - myfilter
                        virtualClusters:
                        - name: demo1
                          targetCluster:
                            bootstrapServers: magic-kafka.example:1234
                          gateways:
                          - name: mygateway
                            portIdentifiesNode:
                              bootstrapAddress: "localhost:9082"
                        """),
                Arguments.argumentSet("Management minimal", """
                        management: {}
                        virtualClusters:
                        - name: demo1
                          targetCluster:
                            bootstrapServers: magic-kafka.example:1234
                          gateways:
                          - name: mygateway
                            portIdentifiesNode:
                              bootstrapAddress: "localhost:9082"
                        """),
                Arguments.argumentSet("Management", """
                        management:
                          bindAddress: 164.0.0.0
                          port: 1000
                          endpoints: {}
                        virtualClusters:
                        - name: demo1
                          targetCluster:
                            bootstrapServers: magic-kafka.example:1234
                          gateways:
                          - name: mygateway
                            portIdentifiesNode:
                              bootstrapAddress: "localhost:9082"
                        """),
                Arguments.argumentSet("Management with Prometheus", """
                        management:
                          endpoints:
                            prometheus: {}
                        virtualClusters:
                        - name: demo1
                          targetCluster:
                            bootstrapServers: magic-kafka.example:1234
                          gateways:
                          - name: mygateway
                            portIdentifiesNode:
                              bootstrapAddress: "localhost:9082"
                        """),
                Arguments.argumentSet("Micrometer", """
                        micrometer:
                        - type: CommonTagsHook
                          config:
                            commonTags:
                              zone: "euc-1a"
                              owner: "becky"
                        virtualClusters:
                        - name: demo1
                          targetCluster:
                            bootstrapServers: magic-kafka.example:1234
                          gateways:
                          - name: mygateway
                            portIdentifiesNode:
                              bootstrapAddress: "localhost:9082"
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
    void testDeserializeFromYaml() {
        Configuration configuration = configParser.parseConfiguration(this.getClass().getClassLoader().getResourceAsStream("config.yaml"));
        assertThat(configuration.isUseIoUring()).isTrue();
        assertThat(configuration.management())
                .isNotNull()
                .satisfies(ahc -> {
                    assertThat(ahc.bindAddress()).isEqualTo("127.0.0.1");
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
                                assertThat(vcl.portIdentifiesNode()).isNotNull().satisfies(
                                        strategy -> {
                                            assertThat(strategy.getBootstrapAddress()).isEqualTo(HostPort.parse("localhost:9192"));
                                        });
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
    void shouldRequireKeyIfDownstreamTlsObjectPresent() {
        // given
        Configuration configuration = configParser.parseConfiguration("""
                virtualClusters:
                  - name: mycluster1
                    targetCluster:
                      bootstrapServers: kafka1.example:1234
                    gateways:
                    - name: default
                      tls: {}
                      portIdentifiesNode:
                        bootstrapAddress: cluster1:9192
                """);
        ServiceBasedPluginFactoryRegistry registry = new ServiceBasedPluginFactoryRegistry();
        // When/Then
        assertThatThrownBy(() -> {
            configuration.virtualClusterModel(registry);
        }).isInstanceOf(IllegalConfigurationException.class)
                .hasMessageStartingWith("Virtual cluster 'mycluster1', gateway 'default': 'tls' object is missing the mandatory attribute 'key'.");
        // We can't assert the full message as the link will change with every release
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
                .hasMessageContaining("Virtual cluster must be unique (case insensitive). The following virtual cluster names are duplicated: [demo1]");
    }

    @Test
    void shouldDetectDuplicateClusterNodeNamesCaseInsensitively() {
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
                  - name: dEmO1
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
                .hasMessageContaining("Virtual cluster must be unique (case insensitive). The following virtual cluster names are duplicated: [demo1]");
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
    void shouldErrorOnAnyUnknownProperties() {
        // Given
        assertThatThrownBy(() ->
        // When
        configParser.parseConfiguration("""
                virtualClusters:
                  - name: demo1
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                      unknownProperty: unknownProperty
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: cluster1:9192
                """))
                // Then
                .isInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("Unrecognized field \"unknownProperty\"");
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

    @ParameterizedTest
    @ValueSource(strings = {
            """
                    virtualClusters: []
                    """,
            """
                    virtualClusters: null
                    """,

    })
    void shouldRequireAtLeastOneVirtualCluster(String config) {
        // Given
        assertThatThrownBy(() ->
        // When
        configParser.parseConfiguration(config))
                // Then
                .isInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("At least one virtual cluster must be defined.");
    }

    @Test
    void testNestedPlugins() {
        ConfigParser cp = new ConfigParser();
        var config = cp.parseConfiguration("""
                filterDefinitions:
                - name: nested
                  type: NestedPluginConfigFactory
                  config:
                    examplePlugin: ExamplePluginInstance
                defaultFilters:
                -  nested
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: magic-kafka.example:1234
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """);
        assertThat(config.filterDefinitions()).hasSize(1);

        NamedFilterDefinition fd = config.filterDefinitions().get(0);
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
                filterDefinitions:
                - name: unknown-plugin
                  type: NestedPluginConfigFactory
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
        for (NamedFilterDefinition fd : config.filterDefinitions()) {
            var pluginFactory = cp.pluginFactory((Class<FilterFactory<? super Object, ? super Object>>) (Class<?>) FilterFactory.class);
            var filterFactory = pluginFactory.pluginInstance(fd.type());
            Class<?> configType = pluginFactory.configType(fd.type());
            assertEquals(expectedConfigType, configType);
            assertTrue(configType.isInstance(fd.config()));
            assertEquals("hello, world", filterFactory.initialize(null, fd.config()));
        }
    }

    static Stream<Arguments> shouldWorkWithDifferentConfigCreators() {
        return Stream.of(Arguments.argumentSet("constructor injection",
                """
                        filterDefinitions:
                        - name: ctor-injection
                          type: ConstructorInjection
                          config:
                            str: hello, world
                        defaultFilters:
                        -  ctor-injection
                        virtualClusters:
                        - name: demo1
                          targetCluster:
                            bootstrapServers: magic-kafka.example:1234
                          gateways:
                          - name: mygateway
                            portIdentifiesNode:
                              bootstrapAddress: "localhost:9082"
                        """,
                ConstructorInjectionConfig.class),
                Arguments.argumentSet("factory method",
                        """
                                filterDefinitions:
                                - name: factory-method
                                  type: FactoryMethod
                                  config:
                                    str: hello, world
                                defaultFilters:
                                -  factory-method
                                virtualClusters:
                                - name: demo1
                                  targetCluster:
                                    bootstrapServers: magic-kafka.example:1234
                                  gateways:
                                  - name: mygateway
                                    portIdentifiesNode:
                                      bootstrapAddress: "localhost:9082"
                                """,
                        FactoryMethodConfig.class),
                Arguments.argumentSet("field injection",
                        """
                                filterDefinitions:
                                - name: field-injection
                                  type: FieldInjection
                                  config:
                                    str: hello, world
                                defaultFilters:
                                -  field-injection
                                virtualClusters:
                                - name: demo1
                                  targetCluster:
                                    bootstrapServers: magic-kafka.example:1234
                                  gateways:
                                  - name: mygateway
                                    portIdentifiesNode:
                                      bootstrapAddress: "localhost:9082"
                                """,
                        FieldInjectionConfig.class),
                Arguments.argumentSet("record",
                        """
                                filterDefinitions:
                                - name: record
                                  type: Record
                                  config:
                                    str: hello, world
                                defaultFilters:
                                -  record
                                virtualClusters:
                                - name: demo1
                                  targetCluster:
                                    bootstrapServers: magic-kafka.example:1234
                                  gateways:
                                  - name: mygateway
                                    portIdentifiesNode:
                                      bootstrapAddress: "localhost:9082"
                                """,
                        RecordConfig.class),
                Arguments.argumentSet("setter injection",
                        """
                                filterDefinitions:
                                - name: setter-injection
                                  type: SetterInjection
                                  config:
                                    str: hello, world
                                defaultFilters:
                                -  setter-injection
                                virtualClusters:
                                - name: demo1
                                  targetCluster:
                                    bootstrapServers: magic-kafka.example:1234
                                  gateways:
                                  - name: mygateway
                                    portIdentifiesNode:
                                      bootstrapAddress: "localhost:9082"
                                """,
                        SetterInjectionConfig.class));
    }

    @Test
    void shouldThrowWhenSerializingUnserializableObject() {
        var targetCluster = new TargetCluster("mycluster:9082", Optional.empty());
        var gateway = new VirtualClusterGateway("gw", new PortIdentifiesNodeIdentificationStrategy(HostPort.parse("localhost:9082"), null, null, null), null,
                Optional.empty());
        var config = new Configuration(null,
                List.of(new NamedFilterDefinition("foo", "", new NonSerializableConfig(""))),
                List.of("foo"),
                List.of(new VirtualCluster("demo", targetCluster, List.of(gateway), false, false, List.of())),
                null,
                false,
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
                filterDefinitions:
                - name: missing-plugin-name
                  type: MissingPluginImplName
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

    @Test
    void shouldSupportDeprecatedManagementConfiguration() {
        // When
        var configurationModel = configParser.parseConfiguration("""
                adminHttp:
                   host: 1.1.1.1
                   port: 1234
                   endpoints:
                     prometheus: {}
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: magic-kafka.example:1234
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """);

        // Then
        assertThat(configurationModel)
                .extracting(Configuration::management)
                .satisfies(m -> {
                    assertThat(m.getEffectivePort()).isEqualTo(1234);
                    assertThat(m.getEffectiveBindAddress()).isEqualTo("1.1.1.1");
                    assertThat(m.endpoints())
                            .extracting(EndpointsConfiguration::maybePrometheus, InstanceOfAssertFactories.optional(PrometheusMetricsConfig.class))
                            .isPresent();
                });
    }

    @Test
    void shouldSupportDeprecatedManagementConfigurationDefaults() {
        // When
        var configurationModel = configParser.parseConfiguration("""
                adminHttp: {}
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: magic-kafka.example:1234
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """);

        // Then
        assertThat(configurationModel)
                .extracting(Configuration::management)
                .satisfies(m -> {
                    assertThat(m.getEffectivePort()).isEqualTo(ManagementConfiguration.DEFAULT_MANAGEMENT_PORT);
                    assertThat(m.getEffectiveBindAddress()).isEqualTo(ManagementConfiguration.DEFAULT_BIND_ADDRESS);
                    assertThat(m.endpoints()).isNull();
                });
    }

    @Test
    void shouldSupportTargetClusterWithDefaultBootstrapServerSelectionStrategy() {
        // When
        var configurationModel = configParser.parseConfiguration("""
                adminHttp: {}
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: magic-kafka.example:1234 
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """);
        // Then
        assertThat(configurationModel)
                .extracting(Configuration::virtualClusters)
                .extracting(virtualClusters -> virtualClusters.get(0))
                .extracting(VirtualCluster::targetCluster)
                .satisfies(targetCluster -> {
                    assertThat(targetCluster.selectionStrategy()).isInstanceOf(FixedBootstrapSelectionStrategy.class);
                    assertThat(targetCluster.bootstrapServer()).isEqualTo(new HostPort("magic-kafka.example", 1234));
                });
    }

    @Test
    void shouldSupportTargetClusterWithFixedBootstrapServerSelectionStrategy() {
        // When
        var configurationModel = configParser.parseConfiguration("""
                adminHttp: {}
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: magic-kafka-0.example:1234,magic-kafka-1.example:1234
                    bootstrapServerSelection:
                        strategy: fixed
                        choice: 1
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """);
        // Then
        assertThat(configurationModel)
                .extracting(Configuration::virtualClusters)
                .extracting(virtualClusters -> virtualClusters.get(0))
                .extracting(VirtualCluster::targetCluster)
                .satisfies(targetCluster -> {
                    assertThat(targetCluster.selectionStrategy()).isInstanceOf(FixedBootstrapSelectionStrategy.class);
                    assertThat(targetCluster.bootstrapServer()).isEqualTo(new HostPort("magic-kafka-1.example", 1234));
                });
    }

    @Test
    void shouldSupportTargetClusterWithRandomBootstrapServerSelectionStrategy() {
        // When
        var configurationModel = configParser.parseConfiguration("""
                adminHttp: {}
                virtualClusters:
                - name: demo1
                  targetCluster:
                    bootstrapServers: magic-kafka-0.example:1234,magic-kafka-1.example:1234,magic-kafka-1.example:1234
                    bootstrapServerSelection:
                        strategy: random
                  gateways:
                  - name: mygateway
                    portIdentifiesNode:
                      bootstrapAddress: "localhost:9082"
                """);
        // Then
        var bootstrapServers = List.of(new HostPort("magic-kafka.example", 1234), new HostPort("magic-kafka-1.example", 1234),
                new HostPort("magic-kafka-2.example", 1234));
        assertThat(configurationModel)
                .extracting(Configuration::virtualClusters)
                .extracting(virtualClusters -> virtualClusters.get(0))
                .extracting(VirtualCluster::targetCluster)
                .satisfies(targetCluster -> {
                    assertThat(targetCluster.selectionStrategy()).isInstanceOf(RandomBootstrapSelectionStrategy.class);
                    assertThat(targetCluster.bootstrapServer()).isIn(bootstrapServers);
                });
    }

    private record NonSerializableConfig(String id) {
        @Override
        @JsonGetter
        public String id() {
            throw new UnsupportedOperationException("boom. haha fooled you jackson");
        }
    }
}
