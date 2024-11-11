/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
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

import io.kroxylicious.proxy.config.secret.PasswordProvider;
import io.kroxylicious.proxy.config.tls.KeyStore;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.internal.filter.ConstructorInjectionConfig;
import io.kroxylicious.proxy.internal.filter.ExamplePluginFactory;
import io.kroxylicious.proxy.internal.filter.FactoryMethodConfig;
import io.kroxylicious.proxy.internal.filter.FieldInjectionConfig;
import io.kroxylicious.proxy.internal.filter.NestedPluginConfigFactory;
import io.kroxylicious.proxy.internal.filter.RecordConfig;
import io.kroxylicious.proxy.internal.filter.SetterInjectionConfig;
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
                              type: PortPerBrokerClusterNetworkAddressConfigProvider
                              config:
                                bootstrapAddress: cluster1:9192
                                numberOfBrokerPorts: 1
                                brokerAddressPattern: localhost
                                brokerStartPort: 9193
                        """),
                Arguments.of("Virtual cluster (RangeAwarePortPerBroker)", """
                        virtualClusters:
                          demo1:
                            targetCluster:
                              bootstrap_servers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: RangeAwarePortPerNodeClusterNetworkAddressConfigProvider
                              config:
                                bootstrapAddress: cluster1:9192
                                nodeAddressPattern: localhost
                                nodeStartPort: 9193
                                nodeIdRanges:
                                  - name: brokers
                                    range:
                                      startInclusive: 0
                                      endExclusive: 3
                        """),
                Arguments.of("Virtual cluster (SniRouting)", """
                        virtualClusters:
                          demo1:
                            targetCluster:
                              bootstrap_servers: kafka.example:1234
                            clusterNetworkAddressConfigProvider:
                              type: SniRoutingClusterNetworkAddressConfigProvider
                              config:
                                bootstrapAddress: cluster1:9192
                                brokerAddressPattern: broker-$(nodeId)
                        """),
                Arguments.of("Downstream/Upstream TLS with inline passwords", """
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
                              type: SniRoutingClusterNetworkAddressConfigProvider
                              config:
                                bootstrapAddress: cluster1:9192
                                brokerAddressPattern: broker-$(nodeId)
                        """),
                Arguments.of("Downstream/Upstream TLS with password files", """
                        virtualClusters:
                          demo1:
                            tls:
                                key:
                                  storeFile: /tmp/foo.jks
                                  storePassword:
                                    passwordFile: /tmp/password.txt
                                  storeType: JKS
                            targetCluster:
                              bootstrap_servers: kafka.example:1234
                              tls:
                                trust:
                                 storeFile: /tmp/foo.jks
                                 storePassword:
                                    passwordFile: /tmp/password.txt
                                 storeType: JKS
                            clusterNetworkAddressConfigProvider:
                              type: SniRoutingClusterNetworkAddressConfigProvider
                              config:
                                bootstrapAddress: cluster1:9192
                                brokerAddressPattern: broker-$(nodeId)
                        """),
                Arguments.of("Filters", """
                        filters:
                        - type: TestFilterFactory
                        """),
                Arguments.of("Admin", """
                        adminHttp:
                          host: 0.0.0.0
                          port: 9193
                          endpoints: {}
                        """),
                Arguments.of("Micrometer", """
                        micrometer:
                        - type: CommonTagsHook
                          config:
                            commonTags:
                              zone: "euc-1a"
                              owner: "becky"
                        """),
                Arguments.of("AdminHttp", """
                        adminHttp:
                          host: kroxy
                          port: 9093
                          endpoints:
                            prometheus: {}
                        """));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void yamlDeserializeSerializeFidelity(String name, String config) throws Exception {
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
        assertThat(configuration.adminHttpConfig())
                .isNotNull()
                .satisfies(ahc -> {
                    assertThat(ahc.host()).isEqualTo("kroxy");
                    assertThat(ahc.port()).isEqualTo(9093);
                    assertThat(ahc.endpoints().maybePrometheus()).isPresent();
                });

        assertThat(configuration.virtualClusters()).hasSize(1);
        assertThat(configuration.virtualClusters().keySet()).containsExactly("demo");
        VirtualCluster cluster = configuration.virtualClusters().values().iterator().next();
        assertThat(cluster.logFrames()).isTrue();
        assertThat(cluster.logNetwork()).isTrue();
        assertThat(cluster.targetCluster()).isNotNull();
        assertThat(cluster.targetCluster().bootstrapServers()).isEqualTo("localhost:9092");
        ClusterNetworkAddressConfigProviderDefinition provider = cluster.clusterNetworkAddressConfigProvider();
        assertThat(provider).isNotNull();
        assertThat(provider.type()).isEqualTo("PortPerBrokerClusterNetworkAddressConfigProvider");
        assertThat(provider.config()).isInstanceOf(PortPerBrokerClusterNetworkAddressConfigProviderConfig.class);
        assertThat(((PortPerBrokerClusterNetworkAddressConfigProviderConfig) provider.config()).getBootstrapAddress()).isEqualTo(HostPort.parse("localhost:9192"));
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
                  myAwesomeCluster:
                    targetCluster:
                      bootstrap_servers: kafka.example:1234
                    clusterNetworkAddressConfigProvider:
                      type: PortPerBrokerClusterNetworkAddressConfigProvider
                      config:
                        bootstrapAddress: cluster1:9192
                        numberOfBrokerPorts: 1
                        brokerAddressPattern: localhost
                        brokerStartPort: 9193
                """);
        // When
        final List<io.kroxylicious.proxy.model.VirtualCluster> actualValidClusters = configurationModel.virtualClusterModel();

        // Then
        assertThat(actualValidClusters).singleElement().extracting("clusterName").isEqualTo("myAwesomeCluster");
    }

    @Test
    void shouldDetectDuplicateClusterNodeNames() {
        // Given
        assertThatThrownBy(() ->
        // When
        configParser.parseConfiguration("""
                virtualClusters:
                  demo1:
                    targetCluster:
                      bootstrap_servers: kafka.example:1234
                    clusterNetworkAddressConfigProvider:
                      type: PortPerBrokerClusterNetworkAddressConfigProvider
                      config:
                        bootstrapAddress: cluster1:9192
                        numberOfBrokerPorts: 1
                        brokerAddressPattern: localhost
                        brokerStartPort: 9193
                  demo1:
                    targetCluster:
                      bootstrap_servers: magic-kafka.example:1234
                    clusterNetworkAddressConfigProvider:
                      type: PortPerBrokerClusterNetworkAddressConfigProvider
                      config:
                        bootstrapAddress: cluster2:9193
                        numberOfBrokerPorts: 1
                        brokerAddressPattern: localhost
                        brokerStartPort: 10193
                """))
                // Then
                .isInstanceOf(IllegalArgumentException.class)
                .hasCauseInstanceOf(JsonMappingException.class) // Debatable to enforce the wrapped JsonMappingException
                .cause()
                .hasMessageStartingWith("Duplicate field 'demo1'");

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
    @ParameterizedTest(name = "{0}")
    @MethodSource
    void shouldWorkWithDifferentConfigCreators(String name, String yaml, Class<?> expectedConfigType) {
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
        return Stream.of(Arguments.of("constructor injection",
                """
                        filters:
                        - type: ConstructorInjection
                          config:
                            str: hello, world
                        """,
                ConstructorInjectionConfig.class),
                Arguments.of("factory method",
                        """
                                filters:
                                - type: FactoryMethod
                                  config:
                                    str: hello, world
                                """,
                        FactoryMethodConfig.class),
                Arguments.of("field injection",
                        """
                                filters:
                                - type: FieldInjection
                                  config:
                                    str: hello, world
                                """,
                        FieldInjectionConfig.class),
                Arguments.of("record",
                        """
                                filters:
                                - type: Record
                                  config:
                                    str: hello, world
                                """,
                        RecordConfig.class),
                Arguments.of("setter injection",
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
        var config = new Configuration(null, null, List.of(new FilterDefinition("", new UnseriasliseableConfig(""))), null, false);

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
    void tlsPasswordConfigurationUnderstandFilePathAlias() throws Exception {

        var password = "mypassword";
        var file = File.createTempFile("pass", "txt");
        file.deleteOnExit();
        Files.writeString(file.toPath(), password);
        final Configuration configurationModel = configParser.parseConfiguration("""
                        virtualClusters:
                          demo1:
                            tls:
                                key:
                                  storeFile: /tmp/store.txt
                                  storePassword:
                                    filePath: %s
                            clusterNetworkAddressConfigProvider:
                              type: PortPerBrokerClusterNetworkAddressConfigProvider
                """.formatted(file.getAbsolutePath()));
        // When
        final var virtualCluster = configurationModel.virtualClusters().values().iterator().next();

        // Then
        assertThat(virtualCluster)
                .extracting(VirtualCluster::tls)
                .extracting(Optional::get)
                .extracting(Tls::key)
                .isInstanceOf(KeyStore.class)
                .asInstanceOf(InstanceOfAssertFactories.type(KeyStore.class))
                .extracting(KeyStore::storePasswordProvider)
                .extracting(PasswordProvider::getProvidedPassword)
                .isEqualTo(password);
    }

    private record UnseriasliseableConfig(String id) {
        @Override
        @JsonGetter
        public String id() {
            throw new UnsupportedOperationException("boom. haha fooled you jackson");
        }
    }
}
