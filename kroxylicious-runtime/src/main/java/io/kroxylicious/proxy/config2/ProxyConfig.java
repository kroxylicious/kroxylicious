/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.NetworkDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.plugin.HasPluginReferences;
import io.kroxylicious.proxy.plugin.PluginReference;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Top-level proxy configuration for config2.
 */
public record ProxyConfig(
                          String version,
                          Path pluginConfigDir,
                          @Nullable ManagementConfiguration management,
                          @Nullable List<FilterFactoryRef> defaultFilters,
                          @JsonProperty(required = true) List<VirtualCluster> virtualClusters,
                          @Nullable List<MicrometerRef> micrometer,
                          boolean useIoUring,
                          Optional<Map<String, Object>> development,
                          @Nullable NetworkDefinition network)
        implements HasPluginReferences {

    private static final String FILTER_FACTORY_INTERFACE = FilterFactory.class.getName();

    @Override
    public Stream<PluginReference<?>> pluginReferences() {
        Stream<PluginReference<?>> defaultFilterRefs = defaultFilters == null
                ? Stream.empty()
                : defaultFilters.stream().map(FilterFactoryRef::toReference);

        Stream<PluginReference<?>> micrometerRefs = micrometer == null
                ? Stream.empty()
                : micrometer.stream().map(MicrometerRef::toReference);

        Stream<PluginReference<?>> vcFilterRefs = virtualClusters.stream()
                .filter(vc -> vc.filters() != null)
                .flatMap(vc -> vc.filters().stream())
                .map(name -> new PluginReference<>(FILTER_FACTORY_INTERFACE, name));

        // TODO: VirtualCluster.subjectBuilder uses the old inline pattern (TransportSubjectBuilderConfig).
        // In config2 this should be a PluginReference<TransportSubjectBuilderService>, but that requires
        // a config2-specific VirtualCluster type.

        return Stream.of(defaultFilterRefs, micrometerRefs, vcFilterRefs)
                .flatMap(s -> s);
    }
}
