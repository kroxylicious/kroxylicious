/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.ObjectMeta;

import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfig;
import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfigSpec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class SidecarConfigResolverTest {

    @Test
    void resolveByExplicitName() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig config = createConfig("ns1", "my-config");
        resolver.put(config);

        Optional<KroxyliciousSidecarConfig> result = resolver.resolve("ns1", "my-config");
        assertThat(result).isPresent().get().isSameAs(config);
    }

    @Test
    void resolveByExplicitNameReturnsEmptyWhenNotFound() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        resolver.put(createConfig("ns1", "my-config"));

        Optional<KroxyliciousSidecarConfig> result = resolver.resolve("ns1", "nonexistent");
        assertThat(result).isEmpty();
    }

    @Test
    void resolvesSingleConfigInNamespace() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig config = createConfig("ns1", "only-one");
        resolver.put(config);

        Optional<KroxyliciousSidecarConfig> result = resolver.resolve("ns1", null);
        assertThat(result).isPresent().get().isSameAs(config);
    }

    @Test
    void returnsEmptyWhenMultipleConfigsAndNoExplicitName() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        resolver.put(createConfig("ns1", "config-a"));
        resolver.put(createConfig("ns1", "config-b"));

        Optional<KroxyliciousSidecarConfig> result = resolver.resolve("ns1", null);
        assertThat(result).isEmpty();
    }

    @Test
    void returnsEmptyWhenNoConfigsInNamespace() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();

        Optional<KroxyliciousSidecarConfig> result = resolver.resolve("ns1", null);
        assertThat(result).isEmpty();
    }

    @Test
    void resolvesFromCorrectNamespace() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig ns1Config = createConfig("ns1", "config");
        KroxyliciousSidecarConfig ns2Config = createConfig("ns2", "config");
        resolver.put(ns1Config);
        resolver.put(ns2Config);

        assertThat(resolver.resolve("ns1", null)).isPresent().get().isSameAs(ns1Config);
        assertThat(resolver.resolve("ns2", null)).isPresent().get().isSameAs(ns2Config);
    }

    @Test
    void explicitNameIgnoresOtherNamespaces() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        resolver.put(createConfig("ns1", "config"));

        Optional<KroxyliciousSidecarConfig> result = resolver.resolve("ns2", "config");
        assertThat(result).isEmpty();
    }

    @Test
    void removeDeletesConfigFromCache() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig config = createConfig("ns1", "my-config");
        resolver.put(config);
        resolver.remove(config);

        assertThat(resolver.resolve("ns1", "my-config")).isEmpty();
    }

    @Test
    void removeCleansUpEmptyNamespaceMap() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        KroxyliciousSidecarConfig config = createConfig("ns1", "config-a");
        resolver.put(config);
        resolver.remove(config);

        KroxyliciousSidecarConfig newConfig = createConfig("ns1", "config-b");
        resolver.put(newConfig);
        assertThat(resolver.resolve("ns1", "config-b")).isPresent().get().isSameAs(newConfig);
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

        assertThat(resolver.resolve("ns1", "config-a")).isPresent().get().isSameAs(kept);
    }

    @Test
    void removeLastConfigThenAddNew() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        resolver.put(createConfig("ns1", "old"));
        resolver.remove(createConfig("ns1", "old"));

        KroxyliciousSidecarConfig replacement = createConfig("ns1", "new");
        resolver.put(replacement);

        assertThat(resolver.resolve("ns1", null)).isPresent().get().isSameAs(replacement);
    }

    @Test
    void closeWithNoInformerDoesNotThrow() {
        SidecarConfigResolver resolver = new SidecarConfigResolver();
        assertThatCode(resolver::close).doesNotThrowAnyException();
    }

    private static KroxyliciousSidecarConfig createConfig(String namespace, String name) {
        KroxyliciousSidecarConfig config = new KroxyliciousSidecarConfig();
        ObjectMeta meta = new ObjectMeta();
        meta.setNamespace(namespace);
        meta.setName(name);
        config.setMetadata(meta);
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setUpstreamBootstrapServers("kafka.example.com:9092");
        config.setSpec(spec);
        return config;
    }
}
