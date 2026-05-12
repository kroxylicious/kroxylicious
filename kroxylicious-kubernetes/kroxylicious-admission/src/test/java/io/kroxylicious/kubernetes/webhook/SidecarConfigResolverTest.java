/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.api.model.ObjectMeta;

import io.kroxylicious.sidecar.v1alpha1.KroxyliciousSidecarConfig;
import io.kroxylicious.sidecar.v1alpha1.KroxyliciousSidecarConfigSpec;
import io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.VirtualClusters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SidecarConfigResolverTest {

    @Mock
    private SidecarConfigStatusUpdater statusUpdater;

    @Test
    void resolveByExplicitName() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig config = createConfig("ns1", "my-config");
        resolver.put(config);

        SidecarConfigResolver.Resolution result = resolver.resolve("ns1", "my-config");
        assertThat(result.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.FOUND);
        assertThat(result.config()).hasValueSatisfying(c -> assertThat(c).isSameAs(config));
    }

    @Test
    void resolveByExplicitNameReturnsNoConfigWhenNotFound() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        resolver.put(createConfig("ns1", "my-config"));

        SidecarConfigResolver.Resolution result = resolver.resolve("ns1", "nonexistent");
        assertThat(result.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.NO_CONFIG);
        assertThat(result.config()).isEmpty();
    }

    @Test
    void resolvesSingleConfigInNamespace() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig config = createConfig("ns1", "only-one");
        resolver.put(config);

        SidecarConfigResolver.Resolution result = resolver.resolve("ns1", null);
        assertThat(result.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.FOUND);
        assertThat(result.config()).hasValueSatisfying(c -> assertThat(c).isSameAs(config));
    }

    @Test
    void returnsMultipleConfigsWhenMultipleAndNoExplicitName() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        resolver.put(createConfig("ns1", "config-a"));
        resolver.put(createConfig("ns1", "config-b"));

        SidecarConfigResolver.Resolution result = resolver.resolve("ns1", null);
        assertThat(result.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.MULTIPLE_CONFIGS);
        assertThat(result.config()).isEmpty();
    }

    @Test
    void returnsNoConfigWhenNoConfigsInNamespace() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();

        SidecarConfigResolver.Resolution result = resolver.resolve("ns1", null);
        assertThat(result.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.NO_CONFIG);
        assertThat(result.config()).isEmpty();
    }

    @Test
    void resolvesFromCorrectNamespace() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig ns1Config = createConfig("ns1", "config");
        KroxyliciousSidecarConfig ns2Config = createConfig("ns2", "config");
        resolver.put(ns1Config);
        resolver.put(ns2Config);

        SidecarConfigResolver.Resolution r1 = resolver.resolve("ns1", null);
        assertThat(r1.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.FOUND);
        assertThat(r1.config()).hasValueSatisfying(c -> assertThat(c).isSameAs(ns1Config));

        SidecarConfigResolver.Resolution r2 = resolver.resolve("ns2", null);
        assertThat(r2.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.FOUND);
        assertThat(r2.config()).hasValueSatisfying(c -> assertThat(c).isSameAs(ns2Config));
    }

    @Test
    void explicitNameIgnoresOtherNamespaces() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        resolver.put(createConfig("ns1", "config"));

        SidecarConfigResolver.Resolution result = resolver.resolve("ns2", "config");
        assertThat(result.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.NO_CONFIG);
        assertThat(result.config()).isEmpty();
    }

    @Test
    void removeDeletesConfigFromCache() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig config = createConfig("ns1", "my-config");
        resolver.put(config);
        resolver.remove(config);

        SidecarConfigResolver.Resolution result = resolver.resolve("ns1", "my-config");
        assertThat(result.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.NO_CONFIG);
    }

    @Test
    void removeCleansUpEmptyNamespaceMap() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig config = createConfig("ns1", "config-a");
        resolver.put(config);
        resolver.remove(config);

        KroxyliciousSidecarConfig newConfig = createConfig("ns1", "config-b");
        resolver.put(newConfig);
        SidecarConfigResolver.Resolution result = resolver.resolve("ns1", "config-b");
        assertThat(result.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.FOUND);
        assertThat(result.config()).hasValueSatisfying(c -> assertThat(c).isSameAs(newConfig));
    }

    @Test
    void removeNonexistentNamespaceDoesNotThrow() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig config = createConfig("ns-unknown", "config");

        assertThatCode(() -> resolver.remove(config)).doesNotThrowAnyException();
    }

    @Test
    void removeNonexistentConfigInExistingNamespace() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig kept = createConfig("ns1", "config-a");
        resolver.put(kept);

        resolver.remove(createConfig("ns1", "config-b"));

        SidecarConfigResolver.Resolution result = resolver.resolve("ns1", "config-a");
        assertThat(result.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.FOUND);
        assertThat(result.config()).hasValueSatisfying(c -> assertThat(c).isSameAs(kept));
    }

    @Test
    void removeLastConfigThenAddNew() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        resolver.put(createConfig("ns1", "old"));
        resolver.remove(createConfig("ns1", "old"));

        KroxyliciousSidecarConfig replacement = createConfig("ns1", "new");
        resolver.put(replacement);

        SidecarConfigResolver.Resolution r = resolver.resolve("ns1", null);
        assertThat(r.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.FOUND);
        assertThat(r.config()).hasValueSatisfying(c -> assertThat(c).isSameAs(replacement));
    }

    @Test
    void closeWithNoInformerDoesNotThrow() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        assertThatCode(resolver::close).doesNotThrowAnyException();
    }

    // --- status updater integration tests ---

    @Test
    void onAddCallsSetReadyForValidConfig() {
        SidecarConfigResolver resolver = new SidecarConfigResolver(statusUpdater);
        KroxyliciousSidecarConfig config = createConfig("ns1", "my-config");

        resolver.simulateAdd(config);

        verify(statusUpdater).setReady(config);
        assertThat(resolver.resolve("ns1", "my-config").outcome())
                .isEqualTo(SidecarConfigResolver.Resolution.Outcome.FOUND);
    }

    @Test
    void onAddCallsSetNotReadyForInvalidConfig() {
        SidecarConfigResolver resolver = new SidecarConfigResolver(statusUpdater);
        KroxyliciousSidecarConfig config = createConfigWithoutBootstrap("ns1", "my-config");

        resolver.simulateAdd(config);

        verify(statusUpdater).setNotReady(
                org.mockito.ArgumentMatchers.eq(config),
                org.mockito.ArgumentMatchers.contains("targetBootstrapServers"));
        assertThat(resolver.resolve("ns1", "my-config").outcome())
                .isEqualTo(SidecarConfigResolver.Resolution.Outcome.INVALID_CONFIG);
    }

    @Test
    void onUpdateCallsSetReadyForValidConfig() {
        SidecarConfigResolver resolver = new SidecarConfigResolver(statusUpdater);
        KroxyliciousSidecarConfig oldConfig = createConfig("ns1", "my-config");
        KroxyliciousSidecarConfig newConfig = createConfig("ns1", "my-config");

        resolver.simulateUpdate(oldConfig, newConfig);

        verify(statusUpdater).setReady(newConfig);
        assertThat(resolver.resolve("ns1", "my-config").outcome())
                .isEqualTo(SidecarConfigResolver.Resolution.Outcome.FOUND);
    }

    @Test
    void onUpdateCallsSetNotReadyForInvalidConfig() {
        SidecarConfigResolver resolver = new SidecarConfigResolver(statusUpdater);
        KroxyliciousSidecarConfig oldConfig = createConfig("ns1", "my-config");
        KroxyliciousSidecarConfig newConfig = createConfigWithoutBootstrap("ns1", "my-config");

        resolver.simulateUpdate(oldConfig, newConfig);

        verify(statusUpdater).setNotReady(
                org.mockito.ArgumentMatchers.eq(newConfig),
                org.mockito.ArgumentMatchers.contains("targetBootstrapServers"));
    }

    @Test
    void noStatusUpdaterDoesNotThrow() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig config = createConfig("ns1", "my-config");

        assertThatCode(() -> resolver.simulateAdd(config)).doesNotThrowAnyException();
        assertThat(resolver.resolve("ns1", "my-config").outcome())
                .isEqualTo(SidecarConfigResolver.Resolution.Outcome.FOUND);
    }

    // --- validation tests ---

    @Test
    void validateAcceptsValidConfig() {
        KroxyliciousSidecarConfig config = createConfig("ns1", "my-config");
        assertThat(SidecarConfigResolver.validate(config)).isEmpty();
    }

    @Test
    void validateRejectsNullSpec() {
        KroxyliciousSidecarConfig config = new KroxyliciousSidecarConfig();
        config.setMetadata(new ObjectMeta());
        config.getMetadata().setNamespace("ns1");
        config.getMetadata().setName("bad");

        assertThat(SidecarConfigResolver.validate(config)).contains("spec is required");
    }

    @Test
    void validateRejectsEmptyVirtualClusters() {
        KroxyliciousSidecarConfig config = new KroxyliciousSidecarConfig();
        config.setMetadata(new ObjectMeta());
        config.getMetadata().setNamespace("ns1");
        config.getMetadata().setName("bad");
        config.setSpec(new KroxyliciousSidecarConfigSpec());
        config.getSpec().setVirtualClusters(java.util.List.of());

        assertThat(SidecarConfigResolver.validate(config))
                .contains("spec.virtualClusters must contain exactly one entry");
    }

    @Test
    void validateRejectsMissingTargetBootstrapServers() {
        KroxyliciousSidecarConfig config = createConfigWithoutBootstrap("ns1", "bad");
        assertThat(SidecarConfigResolver.validate(config))
                .contains("spec.virtualClusters[0].targetBootstrapServers is required");
    }

    @Test
    void validateRejectsBootstrapPortEqualsManagementPort() {
        KroxyliciousSidecarConfig config = createConfig("ns1", "bad");
        config.getSpec().getVirtualClusters().get(0).setBootstrapPort(9082L);
        config.getSpec().setManagementPort(9082L);
        assertThat(SidecarConfigResolver.validate(config))
                .anyMatch(e -> e.contains("must not equal spec.managementPort"));
    }

    @Test
    void validateRejectsBrokerPortRangeExceeding65535() {
        KroxyliciousSidecarConfig config = createConfig("ns1", "bad");
        config.getSpec().getVirtualClusters().get(0).setBootstrapPort(65530L);
        var nodeIdRange = new io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.virtualclusters.NodeIdRange();
        nodeIdRange.setStartInclusive(0L);
        nodeIdRange.setEndInclusive(10L);
        config.getSpec().getVirtualClusters().get(0).setNodeIdRange(nodeIdRange);
        assertThat(SidecarConfigResolver.validate(config))
                .anyMatch(e -> e.contains("exceeds maximum port 65535"));
    }

    @Test
    void validateRejectsManagementPortInBrokerPortRange() {
        KroxyliciousSidecarConfig config = createConfig("ns1", "bad");
        config.getSpec().getVirtualClusters().get(0).setBootstrapPort(9090L);
        var nodeIdRange = new io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.virtualclusters.NodeIdRange();
        nodeIdRange.setStartInclusive(0L);
        nodeIdRange.setEndInclusive(4L);
        config.getSpec().getVirtualClusters().get(0).setNodeIdRange(nodeIdRange);
        // broker ports: 9091-9095, management port 9093 conflicts
        config.getSpec().setManagementPort(9093L);
        assertThat(SidecarConfigResolver.validate(config))
                .anyMatch(e -> e.contains("conflicts with broker port range"));
    }

    @Test
    void validateRejectsInvertedNodeIdRange() {
        KroxyliciousSidecarConfig config = createConfig("ns1", "bad");
        var nodeIdRange = new io.kroxylicious.sidecar.v1alpha1.kroxylicioussidecarconfigspec.virtualclusters.NodeIdRange();
        nodeIdRange.setStartInclusive(5L);
        nodeIdRange.setEndInclusive(2L);
        config.getSpec().getVirtualClusters().get(0).setNodeIdRange(nodeIdRange);
        assertThat(SidecarConfigResolver.validate(config))
                .anyMatch(e -> e.contains("must not exceed endInclusive"));
    }

    @Test
    void validateAcceptsManagementPortOutsideBrokerRange() {
        KroxyliciousSidecarConfig config = createConfig("ns1", "ok");
        config.getSpec().getVirtualClusters().get(0).setBootstrapPort(9092L);
        config.getSpec().setManagementPort(9082L);
        assertThat(SidecarConfigResolver.validate(config)).isEmpty();
    }

    @Test
    void resolveReturnsInvalidConfigWhenSingleConfigIsInvalid() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig config = createConfigWithoutBootstrap("ns1", "bad");
        resolver.put(config);

        SidecarConfigResolver.Resolution result = resolver.resolve("ns1", null);
        assertThat(result.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.INVALID_CONFIG);
        assertThat(result.config()).isEmpty();
    }

    @Test
    void resolveReturnsInvalidConfigWhenExplicitNamedConfigIsInvalid() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig config = createConfigWithoutBootstrap("ns1", "bad");
        resolver.put(config);

        SidecarConfigResolver.Resolution result = resolver.resolve("ns1", "bad");
        assertThat(result.outcome()).isEqualTo(SidecarConfigResolver.Resolution.Outcome.INVALID_CONFIG);
        assertThat(result.config()).isEmpty();
    }

    private static KroxyliciousSidecarConfig createConfig(String namespace, String name) {
        KroxyliciousSidecarConfig config = new KroxyliciousSidecarConfig();
        ObjectMeta meta = new ObjectMeta();
        meta.setNamespace(namespace);
        meta.setName(name);
        config.setMetadata(meta);
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        VirtualClusters vc = new VirtualClusters();
        vc.setName("sidecar");
        vc.setTargetBootstrapServers("kafka.example.com:9092");
        spec.setVirtualClusters(java.util.List.of(vc));
        config.setSpec(spec);
        return config;
    }

    private static KroxyliciousSidecarConfig createConfigWithoutBootstrap(String namespace, String name) {
        KroxyliciousSidecarConfig config = new KroxyliciousSidecarConfig();
        ObjectMeta meta = new ObjectMeta();
        meta.setNamespace(namespace);
        meta.setName(name);
        config.setMetadata(meta);
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        VirtualClusters vc = new VirtualClusters();
        vc.setName("sidecar");
        spec.setVirtualClusters(java.util.List.of(vc));
        config.setSpec(spec);
        return config;
    }
}
