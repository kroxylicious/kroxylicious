/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.MicrometerDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NetworkDefinition;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.ProxyProtocolConfig;
import io.kroxylicious.proxy.config.ProxyProtocolMode;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.config.admin.EndpointsConfiguration;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.config.admin.PrometheusMetricsConfig;
import io.kroxylicious.proxy.micrometer.CommonTagsHook;
import io.kroxylicious.proxy.micrometer.PauseDetectorHook;
import io.kroxylicious.proxy.micrometer.StandardBindersHook;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class StaticSectionDifferTest {

    private final StaticSectionDiffer differ = new StaticSectionDiffer();

    @Test
    void identicalConfigsProduceEmptyDiff() {
        var config = baseConfig();
        assertThat(differ.diff(config, config)).isEmpty();
    }

    @Test
    void differingManagementSectionIsDetected() {
        var oldConfig = baseConfig();
        var newConfig = withManagement(oldConfig, new ManagementConfiguration(null, null, null));
        assertThat(differ.diff(oldConfig, newConfig)).containsExactly("management");
    }

    @Test
    void managementSectionsWithEquivalentPrometheusConfigsDoNotDiff() {
        // A reconfigure that re-parses configuration produces a fresh PrometheusMetricsConfig
        // instance. Unless that marker config has value equality, the management section would be
        // flagged as a static change on every reconfigure of a metrics-enabled proxy, rejecting
        // otherwise-valid hot reloads.
        var oldConfig = withManagement(baseConfig(),
                new ManagementConfiguration(null, null, new EndpointsConfiguration(new PrometheusMetricsConfig())));
        var newConfig = withManagement(baseConfig(),
                new ManagementConfiguration(null, null, new EndpointsConfiguration(new PrometheusMetricsConfig())));
        assertThat(differ.diff(oldConfig, newConfig)).isEmpty();
    }

    static Stream<Arguments> equivalentHookConfigs() {
        // Each supplier produces a fresh-but-equal plugin config instance, mimicking what re-parsing
        // configuration yields. Covers every built-in micrometer hook config type.
        return Stream.of(
                argumentSet("CommonTagsHook", (Supplier<Object>) () -> new CommonTagsHook.CommonTagsHookConfig(Map.of("zone", "a"))),
                argumentSet("StandardBindersHook", (Supplier<Object>) () -> new StandardBindersHook.StandardBindersHookConfig(List.of("UptimeMetrics"))),
                argumentSet("PauseDetectorHook", (Supplier<Object>) () -> new PauseDetectorHook.PauseDetectorHookConfig(250L, 250L)));
    }

    @ParameterizedTest
    @MethodSource("equivalentHookConfigs")
    void micrometerSectionsWithEquivalentHookConfigsDoNotDiff(Supplier<Object> configFactory) {
        // Re-parsing configuration yields a fresh plugin config instance each time. Unless those
        // plugin config types have value equality, the micrometer section would be flagged as a
        // static change on every reconfigure of a proxy with micrometer hooks configured.
        var oldConfig = withMicrometer(baseConfig(), List.of(new MicrometerDefinition("hook", configFactory.get())));
        var newConfig = withMicrometer(baseConfig(), List.of(new MicrometerDefinition("hook", configFactory.get())));
        assertThat(differ.diff(oldConfig, newConfig)).isEmpty();
    }

    @Test
    void differingUseIoUringIsDetected() {
        var oldConfig = baseConfig();
        var newConfig = new Configuration(oldConfig.management(), oldConfig.clusterDefinitions(), oldConfig.filterDefinitions(),
                oldConfig.defaultFilters(), oldConfig.virtualClusters(), oldConfig.micrometer(),
                !oldConfig.useIoUring(), // toggled
                oldConfig.development(), oldConfig.network(), oldConfig.proxyProtocol());
        assertThat(differ.diff(oldConfig, newConfig)).containsExactly("useIoUring");
    }

    @Test
    void differingProxyProtocolIsDetected() {
        var oldConfig = baseConfig();
        var newConfig = withProxyProtocol(oldConfig, new ProxyProtocolConfig(ProxyProtocolMode.REQUIRED));
        assertThat(differ.diff(oldConfig, newConfig)).containsExactly("proxyProtocol");
    }

    @Test
    void differingMicrometerIsDetected() {
        var oldConfig = baseConfig();
        var newConfig = new Configuration(oldConfig.management(), oldConfig.clusterDefinitions(), oldConfig.filterDefinitions(),
                oldConfig.defaultFilters(), oldConfig.virtualClusters(),
                List.of(new MicrometerDefinition("SomeMicrometerType", null)), // different from base's null
                oldConfig.useIoUring(), oldConfig.development(), oldConfig.network(), oldConfig.proxyProtocol());
        assertThat(differ.diff(oldConfig, newConfig)).containsExactly("micrometer");
    }

    @Test
    void differingNetworkIsDetected() {
        var oldConfig = baseConfig();
        var newConfig = withNetwork(oldConfig, new NetworkDefinition(null, null));
        assertThat(differ.diff(oldConfig, newConfig)).containsExactly("network");
    }

    @Test
    void differingDevelopmentIsDetected() {
        var oldConfig = baseConfig();
        var newConfig = new Configuration(oldConfig.management(), oldConfig.clusterDefinitions(), oldConfig.filterDefinitions(),
                oldConfig.defaultFilters(), oldConfig.virtualClusters(), oldConfig.micrometer(),
                oldConfig.useIoUring(),
                Optional.of(Map.of("debug", "true")), // different from base's Optional.empty()
                oldConfig.network(), oldConfig.proxyProtocol());
        assertThat(differ.diff(oldConfig, newConfig)).containsExactly("development");
    }

    @Test
    void multipleDifferingSectionsAreAllReported() {
        var oldConfig = baseConfig();
        var newConfig = new Configuration(
                new ManagementConfiguration(null, null, null), // changed: management
                oldConfig.clusterDefinitions(),
                oldConfig.filterDefinitions(),
                oldConfig.defaultFilters(),
                oldConfig.virtualClusters(),
                oldConfig.micrometer(),
                !oldConfig.useIoUring(), // changed: useIoUring
                oldConfig.development(),
                oldConfig.network(),
                new ProxyProtocolConfig(ProxyProtocolMode.REQUIRED)); // changed: proxyProtocol
        assertThat(differ.diff(oldConfig, newConfig))
                .containsExactlyInAnyOrder("management", "useIoUring", "proxyProtocol");
    }

    @Test
    void changesToReconcilableSectionsAreIgnored() {
        // virtualClusters, filterDefinitions, defaultFilters all changed — but none are
        // static, so the differ reports no diff.
        var oldConfig = baseConfig();
        var newConfig = new Configuration(
                oldConfig.management(),
                oldConfig.clusterDefinitions(),
                List.of(new NamedFilterDefinition("filter-a", "SomeFilterType", null)), // changed: filterDefinitions
                List.of("filter-a"), // changed: defaultFilters
                List.of(vc("different-cluster")), // changed: virtualClusters
                oldConfig.micrometer(),
                oldConfig.useIoUring(),
                oldConfig.development(),
                oldConfig.network(),
                oldConfig.proxyProtocol());
        assertThat(differ.diff(oldConfig, newConfig)).isEmpty();
    }

    @Test
    void diffRejectsNullOldConfig() {
        var config = baseConfig();
        assertThatThrownBy(() -> differ.diff(null, config))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("oldConfig");
    }

    @Test
    void diffRejectsNullNewConfig() {
        var config = baseConfig();
        assertThatThrownBy(() -> differ.diff(config, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("newConfig");
    }

    @Test
    void diffResultIsImmutable() {
        var oldConfig = baseConfig();
        var newConfig = withManagement(oldConfig, new ManagementConfiguration(null, null, null));
        var diffs = differ.diff(oldConfig, newConfig);
        assertThatThrownBy(() -> diffs.add("extra"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // -------- fixture helpers --------

    private static Configuration baseConfig() {
        return new Configuration(null, null, null,
                List.of(vc("base-cluster")), null,
                false, Optional.empty(), null, null);
    }

    private static Configuration withManagement(Configuration base, ManagementConfiguration management) {
        return new Configuration(management, base.clusterDefinitions(), base.filterDefinitions(), base.defaultFilters(),
                base.virtualClusters(), base.micrometer(),
                base.useIoUring(), base.development(), base.network(), base.proxyProtocol());
    }

    private static Configuration withMicrometer(Configuration base, List<MicrometerDefinition> micrometer) {
        return new Configuration(base.management(), base.filterDefinitions(), base.defaultFilters(),
                base.virtualClusters(), micrometer,
                base.useIoUring(), base.development(), base.network(), base.proxyProtocol());
    }

    private static Configuration withNetwork(Configuration base, NetworkDefinition network) {
        return new Configuration(base.management(), base.clusterDefinitions(), base.filterDefinitions(), base.defaultFilters(),
                base.virtualClusters(), base.micrometer(),
                base.useIoUring(), base.development(), network, base.proxyProtocol());
    }

    private static Configuration withProxyProtocol(Configuration base, ProxyProtocolConfig proxyProtocol) {
        return new Configuration(base.management(), base.clusterDefinitions(), base.filterDefinitions(), base.defaultFilters(),
                base.virtualClusters(), base.micrometer(),
                base.useIoUring(), base.development(), base.network(), proxyProtocol);
    }

    private static VirtualCluster vc(String name) {
        var gateway = new VirtualClusterGateway("default",
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9192), null, null, null),
                null,
                Optional.empty());
        return new VirtualCluster(name,
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway),
                false, false, List.of());
    }
}
