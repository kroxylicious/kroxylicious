/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.plugin.HasPluginReferences;
import io.kroxylicious.proxy.plugin.PluginReference;
import io.kroxylicious.proxy.plugin.Stateless;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProxyConfigParserTest {

    private static final String FILTER_FACTORY_INTERFACE = "io.kroxylicious.proxy.filter.FilterFactory";
    private static final String REQUIRES_CONFIG = "com.example.plugins.RequiresConfigFactory";
    private static final String MULTI_VERSION = "com.example.plugins.MultiVersionFactory";
    private static final String STATEFUL = "com.example.plugins.StatefulFactory";
    private static final String STATELESS = "com.example.plugins.StatelessFactory";
    private static final String NO_REFS = "com.example.plugins.NoRefsFactory";

    private ProxyConfigParser parser;

    /** Simple config type for testing — implements HasPluginReferences with no references. */
    public record TestConfig(String foo) implements HasPluginReferences {
        @Override
        public Stream<PluginReference<?>> pluginReferences() {
            return Stream.empty();
        }
    }

    /** Config type for v2 of the test plugin. */
    public record TestConfigV2(String foo, int bar) implements HasPluginReferences {
        @Override
        public Stream<PluginReference<?>> pluginReferences() {
            return Stream.empty();
        }
    }

    /** Config type that does NOT implement HasPluginReferences — for negative tests. */
    public record BadConfig(String foo) {}

    /** Simulates a stateful plugin implementation (no @Stateless). */
    static class StatefulFactory {
    }

    /** Simulates a stateless plugin implementation. */
    @Stateless
    static class StatelessFactory {
    }

    @BeforeEach
    void setUp() {
        PluginFactoryRegistry registry = new PluginFactoryRegistry() {
            @SuppressWarnings("unchecked")
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                if (pluginClass == FilterFactory.class) {
                    return (PluginFactory<P>) testFilterFactory();
                }
                throw new IllegalArgumentException("Unknown plugin class: " + pluginClass);
            }
        };
        parser = new ProxyConfigParser(registry);
    }

    private static PluginFactory<FilterFactory> testFilterFactory() {
        return new PluginFactory<>() {
            @Override
            public FilterFactory pluginInstance(String instanceName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Class<?> configType(String instanceName) {
                return TestConfig.class;
            }

            @Override
            public Map<String, Class<?>> configVersions(String instanceName) {
                return switch (instanceName) {
                    case REQUIRES_CONFIG, STATEFUL, STATELESS -> Map.of("v1alpha1", TestConfig.class);
                    case MULTI_VERSION -> Map.of("v1", TestConfig.class, "v2", TestConfigV2.class);
                    case NO_REFS -> Map.of("v1alpha1", BadConfig.class);
                    default -> throw new IllegalArgumentException("Unknown: " + instanceName);
                };
            }

            @Override
            public Class<?> implementationType(String instanceName) {
                return switch (instanceName) {
                    case REQUIRES_CONFIG, MULTI_VERSION, NO_REFS -> StatefulFactory.class;
                    case STATEFUL -> StatefulFactory.class;
                    case STATELESS -> StatelessFactory.class;
                    default -> throw new IllegalArgumentException("Unknown: " + instanceName);
                };
            }

            @Override
            public Set<String> registeredInstanceNames() {
                return Set.of(REQUIRES_CONFIG, MULTI_VERSION, STATEFUL, STATELESS, NO_REFS);
            }
        };
    }

    // --- Version detection ---

    @Test
    void legacyConfigWithoutVersionDelegatesToConfigParser() {
        // Legacy configs have no version field — ProxyConfigParser delegates to ConfigParser.
        // ConfigParser requires at least one virtual cluster, so an empty list triggers
        // its validation. The specific error proves delegation happened.
        var snapshot = TestSnapshot.builder("""
                adminHttp:
                  endpoints: {}
                virtualClusters: []
                """)
                .build();

        assertThatThrownBy(() -> parser.parse(snapshot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("At least one virtual cluster must be defined.");
    }

    private static final String MINIMAL_VC = """
            virtualClusters:
              - name: test-vc
                targetCluster:
                  bootstrapServers: localhost:9092
                gateways:
                  - name: gw
                    portIdentifiesNode:
                      bootstrapAddress: localhost:9192
            """;

    @Test
    void versionedConfigReturnsConfiguration() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                %s
                defaultFilters:
                  - my-filter
                """.formatted(MINIMAL_VC))
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: com.example.plugins.RequiresConfigFactory
                        version: v1alpha1
                        config:
                          foo: hello
                        """)
                .build();

        Configuration result = parser.parse(snapshot);

        assertThat(result).isNotNull();
        assertThat(result.virtualClusters()).hasSize(1);
        assertThat(result.virtualClusters().get(0).name()).isEqualTo("test-vc");
        assertThat(result.filterDefinitions()).hasSize(1);
        assertThat(result.filterDefinitions().get(0).name()).isEqualTo("my-filter");
        assertThat(result.filterDefinitions().get(0).type()).isEqualTo(REQUIRES_CONFIG);
        assertThat(result.filterDefinitions().get(0).config()).isInstanceOf(TestConfig.class);
        assertThat(((TestConfig) result.filterDefinitions().get(0).config()).foo()).isEqualTo("hello");
        assertThat(result.defaultFilters()).containsExactly("my-filter");
    }

    @Test
    void versionedConfigWithVirtualClusterFilters() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters:
                  - name: test-vc
                    targetCluster:
                      bootstrapServers: localhost:9092
                    gateways:
                      - name: gw
                        portIdentifiesNode:
                          bootstrapAddress: localhost:9192
                    filters:
                      - my-filter
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: com.example.plugins.RequiresConfigFactory
                        version: v1alpha1
                        config:
                          foo: bar
                        """)
                .build();

        Configuration result = parser.parse(snapshot);

        assertThat(result).isNotNull();
        assertThat(result.virtualClusters().get(0).filters()).containsExactly("my-filter");
        assertThat(result.filterDefinitions()).hasSize(1);
    }

    @Test
    void versionedConfigWithNoFiltersReturnsConfiguration() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                %s
                """.formatted(MINIMAL_VC))
                .build();

        Configuration result = parser.parse(snapshot);

        assertThat(result).isNotNull();
        assertThat(result.filterDefinitions()).isNull();
        assertThat(result.defaultFilters()).isNull();
    }

    // --- resolveAll / loadAllPluginConfigs ---

    @Test
    void resolveAllDeserializesPluginConfig() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: com.example.plugins.RequiresConfigFactory
                        version: v1alpha1
                        config:
                          foo: bar
                        """)
                .build();

        Map<String, Map<String, ResolvedPluginConfig>> resolved = parser.resolveAll(snapshot);

        assertThat(resolved).containsKey(FILTER_FACTORY_INTERFACE);
        Map<String, ResolvedPluginConfig> filters = resolved.get(FILTER_FACTORY_INTERFACE);
        assertThat(filters).containsKey("my-filter");

        ResolvedPluginConfig rpc = filters.get("my-filter");
        assertThat(rpc.pluginInterfaceName()).isEqualTo(FILTER_FACTORY_INTERFACE);
        assertThat(rpc.pluginInstanceName()).isEqualTo("my-filter");
        assertThat(rpc.pc().type()).isEqualTo(REQUIRES_CONFIG);
        assertThat(rpc.pc().version()).isEqualTo("v1alpha1");
        assertThat(rpc.pc().config()).isInstanceOf(TestConfig.class);
        assertThat(((TestConfig) rpc.pc().config()).foo()).isEqualTo("bar");
    }

    @Test
    void resolveAllHandlesMultipleInstances() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: filter-a
                        type: com.example.plugins.RequiresConfigFactory
                        version: v1alpha1
                        config:
                          foo: alpha
                        """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: filter-b
                        type: com.example.plugins.RequiresConfigFactory
                        version: v1alpha1
                        config:
                          foo: bravo
                        """)
                .build();

        Map<String, Map<String, ResolvedPluginConfig>> resolved = parser.resolveAll(snapshot);

        Map<String, ResolvedPluginConfig> filters = resolved.get(FILTER_FACTORY_INTERFACE);
        assertThat(filters).hasSize(2);
        assertThat(((TestConfig) filters.get("filter-a").pc().config()).foo()).isEqualTo("alpha");
        assertThat(((TestConfig) filters.get("filter-b").pc().config()).foo()).isEqualTo("bravo");
    }

    @Test
    void resolveAllSelectsCorrectConfigTypeForVersion() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: com.example.plugins.MultiVersionFactory
                        version: v2
                        config:
                          foo: hello
                          bar: 42
                        """)
                .build();

        Map<String, Map<String, ResolvedPluginConfig>> resolved = parser.resolveAll(snapshot);
        ResolvedPluginConfig rpc = resolved.get(FILTER_FACTORY_INTERFACE).get("my-filter");

        assertThat(rpc.pc().config()).isInstanceOf(TestConfigV2.class);
        TestConfigV2 config = (TestConfigV2) rpc.pc().config();
        assertThat(config.foo()).isEqualTo("hello");
        assertThat(config.bar()).isEqualTo(42);
    }

    // --- Error cases ---

    @Test
    void resolveAllRejectsUnsupportedVersion() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: com.example.plugins.RequiresConfigFactory
                        version: v99
                        config: {}
                        """)
                .build();

        assertThatThrownBy(() -> parser.resolveAll(snapshot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("v99")
                .hasMessageContaining(REQUIRES_CONFIG);
    }

    @Test
    void resolveAllRejectsShortTypeName() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: RequiresConfigFactory
                        version: v1alpha1
                        config: {}
                        """)
                .build();

        assertThatThrownBy(() -> parser.resolveAll(snapshot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a fully qualified class name");
    }

    @Test
    void resolveAllRejectsUnknownPluginInterface() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance("com.example.NoSuchInterface", """
                        name: instance
                        type: com.example.plugins.Foo
                        version: v1
                        config: {}
                        """)
                .build();

        assertThatThrownBy(() -> parser.resolveAll(snapshot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("com.example.NoSuchInterface");
    }

    // --- Shared / @Stateless validation ---

    @Test
    void resolveAllRejectsSharedOnNonStatelessPlugin() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: com.example.plugins.StatefulFactory
                        version: v1alpha1
                        shared: true
                        config:
                          foo: bar
                        """)
                .build();

        assertThatThrownBy(() -> parser.resolveAll(snapshot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shared: true")
                .hasMessageContaining("@Stateless");
    }

    @Test
    void resolveAllRejectsVersionedConfigWithoutHasPluginReferences() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: com.example.plugins.NoRefsFactory
                        version: v1alpha1
                        config:
                          foo: bar
                        """)
                .build();

        assertThatThrownBy(() -> parser.resolveAll(snapshot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HasPluginReferences");
    }

    @Test
    void resolveAllAcceptsSharedOnStatelessPlugin() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: com.example.plugins.StatelessFactory
                        version: v1alpha1
                        shared: true
                        config:
                          foo: bar
                        """)
                .build();

        Map<String, Map<String, ResolvedPluginConfig>> resolved = parser.resolveAll(snapshot);
        ResolvedPluginConfig rpc = resolved.get(FILTER_FACTORY_INTERFACE).get("my-filter");
        assertThat(rpc.pc().shared()).isTrue();
    }

    @Test
    void resolveAllAcceptsNonSharedOnStatefulPlugin() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: com.example.plugins.StatefulFactory
                        version: v1alpha1
                        config:
                          foo: bar
                        """)
                .build();

        Map<String, Map<String, ResolvedPluginConfig>> resolved = parser.resolveAll(snapshot);
        ResolvedPluginConfig rpc = resolved.get(FILTER_FACTORY_INTERFACE).get("my-filter");
        assertThat(rpc.pc().shared()).isFalse();
    }

    @Test
    void resolveAllAcceptsPluginWithNoConfigProperty() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: com.example.plugins.StatefulFactory
                        version: v1alpha1
                        """)
                .build();

        Map<String, Map<String, ResolvedPluginConfig>> resolved = parser.resolveAll(snapshot);
        ResolvedPluginConfig rpc = resolved.get(FILTER_FACTORY_INTERFACE).get("my-filter");
        assertThat(rpc.pc().config()).isNull();
    }

    // --- $schema and schema validation ---

    @Test
    void dollarSchemaFieldDoesNotBreakDeserialization() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        $schema: https://example.com/schema.yaml
                        name: my-filter
                        type: com.example.plugins.RequiresConfigFactory
                        version: v1alpha1
                        config:
                          foo: hello
                        """)
                .build();

        Map<String, Map<String, ResolvedPluginConfig>> resolved = parser.resolveAll(snapshot);
        assertThat(resolved.get(FILTER_FACTORY_INTERFACE)).containsKey("my-filter");
    }

    @Test
    void schemaValidationRejectsInvalidConfig() {
        // RequiresConfigFactory/v1alpha1 schema requires foo as string, rejects additional properties
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: com.example.plugins.RequiresConfigFactory
                        version: v1alpha1
                        config:
                          foo: hello
                          unexpected: bad
                        """)
                .build();

        assertThatThrownBy(() -> parser.resolveAll(snapshot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-filter")
                .hasMessageContaining("unexpected");
    }

    @Test
    void pluginWithoutSchemaProceeds() {
        // MultiVersionFactory has no schema on classpath — should parse without error
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .addPluginInstance(FILTER_FACTORY_INTERFACE, """
                        name: my-filter
                        type: com.example.plugins.MultiVersionFactory
                        version: v1
                        config:
                          foo: hello
                        """)
                .build();

        Map<String, Map<String, ResolvedPluginConfig>> resolved = parser.resolveAll(snapshot);
        assertThat(resolved.get(FILTER_FACTORY_INTERFACE)).containsKey("my-filter");
    }

    @Test
    void resolveAllWithEmptySnapshotReturnsEmptyMap() {
        var snapshot = TestSnapshot.builder("""
                version: v1alpha1
                virtualClusters: []
                """)
                .build();

        Map<String, Map<String, ResolvedPluginConfig>> resolved = parser.resolveAll(snapshot);
        assertThat(resolved).isEmpty();
    }

    // --- TestSnapshot ---

    @Test
    void testSnapshotReturnsProxyConfig() {
        String yaml = "version: v1\nvirtualClusters: []";
        var snapshot = TestSnapshot.builder(yaml).build();
        assertThat(snapshot.proxyConfig()).isEqualTo(yaml);
    }

    @Test
    void testSnapshotReturnsPluginInterfaces() {
        var snapshot = TestSnapshot.builder("version: v1")
                .addPluginInstance("com.example.A", "name: inst1")
                .addPluginInstance("com.example.B", "name: inst2")
                .build();

        assertThat(snapshot.pluginInterfaces())
                .containsExactlyInAnyOrder("com.example.A", "com.example.B");
    }

    @Test
    void testSnapshotReturnsPluginInstances() {
        var snapshot = TestSnapshot.builder("version: v1")
                .addPluginInstance("com.example.A", "name: inst1")
                .addPluginInstance("com.example.A", "name: inst2")
                .build();

        assertThat(snapshot.pluginInstances("com.example.A"))
                .containsExactlyInAnyOrder("inst1", "inst2");
    }

    @Test
    void testSnapshotReturnsPluginInstanceContent() {
        String yaml = "name: inst1\ntype: com.example.Impl\nversion: v1\nshared: true";
        var snapshot = TestSnapshot.builder("version: v1")
                .addPluginInstance("com.example.A", yaml)
                .build();

        PluginInstanceContent content = snapshot.pluginInstance("com.example.A", "inst1");
        assertThat(content.metadata().name()).isEqualTo("inst1");
        assertThat(content.metadata().type()).isEqualTo("com.example.Impl");
        assertThat(content.metadata().version()).isEqualTo("v1");
        assertThat(content.metadata().shared()).isTrue();
        assertThat(content.metadata().generation()).isPositive();
        assertThat(new String(content.data(), StandardCharsets.UTF_8)).isEqualTo(yaml);
    }

    @Test
    void testSnapshotThrowsForMissingInterface() {
        var snapshot = TestSnapshot.builder("version: v1").build();

        assertThatThrownBy(() -> snapshot.pluginInstance("com.example.Missing", "x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSnapshotThrowsForMissingInstance() {
        var snapshot = TestSnapshot.builder("version: v1")
                .addPluginInstance("com.example.A", "name: inst1")
                .build();

        assertThatThrownBy(() -> snapshot.pluginInstance("com.example.A", "missing"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSnapshotEmptyPluginInstancesForUnknownInterface() {
        var snapshot = TestSnapshot.builder("version: v1").build();

        assertThat(snapshot.pluginInstances("com.example.Unknown")).isEmpty();
    }

    @Test
    void testSnapshotAcceptsClassLiteral() {
        var snapshot = TestSnapshot.builder("version: v1")
                .addPluginInstance(FilterFactory.class, "name: f1")
                .build();

        assertThat(snapshot.pluginInterfaces()).containsExactly(FILTER_FACTORY_INTERFACE);
        assertThat(snapshot.pluginInstance(FILTER_FACTORY_INTERFACE, "f1").metadata().name())
                .isEqualTo("f1");
    }

    @Test
    void testSnapshotSupportsBinaryPluginInstances() {
        byte[] binaryData = { 0x30, (byte) 0x82, 0x01 };
        var snapshot = TestSnapshot.builder("version: v1")
                .addBinaryPluginInstance("com.example.KeyMaterial", "my-keystore",
                        "com.example.KeyStoreProvider", "v1", binaryData)
                .build();

        assertThat(snapshot.pluginInstances("com.example.KeyMaterial"))
                .containsExactly("my-keystore");

        PluginInstanceContent content = snapshot.pluginInstance(
                "com.example.KeyMaterial", "my-keystore");
        assertThat(content.metadata().name()).isEqualTo("my-keystore");
        assertThat(content.metadata().type()).isEqualTo("com.example.KeyStoreProvider");
        assertThat(content.data()).isEqualTo(binaryData);
    }

    @Test
    void testSnapshotSupportsPasswords() {
        var snapshot = TestSnapshot.builder("version: v1")
                .addPassword("my-keystore", "changeit")
                .build();

        assertThat(snapshot.resourcePassword("my-keystore")).isEqualTo("changeit".toCharArray());
        assertThat(snapshot.resourcePassword("unknown")).isNull();
    }
}
