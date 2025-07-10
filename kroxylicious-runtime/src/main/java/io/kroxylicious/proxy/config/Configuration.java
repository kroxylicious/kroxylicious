/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.type.TypeFactory;

import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.SniRoutingClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The root of the proxy configuration.
 * <br>
 * <br>
 * Note that {@code adminHttp} is accepted as an alias for {@code management}.  Use of {@code adminHttp} is deprecated since 0.11.0
 * and will be removed in a future release.
 *
 * @param management management configuration
 * @param filterDefinitions A list of named filter definitions (names must be unique)
 * @param defaultFilters The names of the {@link #filterDefinitions()} to be use when a {@link VirtualCluster} doesn't specify its own {@link VirtualCluster#filters()}.
 * @param virtualClusters The virtual clusters
 * @param micrometer The micrometer config
 * @param useIoUring true to use iouring
 * @param development Development options
 */
@JsonPropertyOrder({ "management", "filterDefinitions", "defaultFilters", "virtualClusters", "micrometer", "useIoUring", "development" })
public record Configuration(
                            @Nullable @JsonAlias("adminHttp") @JsonDeserialize(using = AdminHttpDeprecationLoggingDeserializer.class) ManagementConfiguration management,
                            @Nullable List<NamedFilterDefinition> filterDefinitions,
                            @Nullable List<String> defaultFilters,
                            @NonNull @JsonProperty(required = true) @JsonDeserialize(using = VirtualClusterContainerDeserializer.class) List<VirtualCluster> virtualClusters,
                            @Nullable List<MicrometerDefinition> micrometer,
                            boolean useIoUring,
                            @NonNull Optional<Map<String, Object>> development) {

    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

    /**
     * Creates an instance of configuration.
     *
     */
    public Configuration {
        Objects.requireNonNull(development);
        if (virtualClusters == null || virtualClusters.isEmpty()) {
            throw new IllegalConfigurationException("At least one virtual cluster must be defined.");
        }

        validateNoDuplicatedClusterNames(virtualClusters);

        // Enforce post condition: filterDefinitions have a unique name
        if (filterDefinitions != null) {
            Map<String, List<NamedFilterDefinition>> groupdByName = filterDefinitions.stream().collect(Collectors.groupingBy(NamedFilterDefinition::name));
            var duplicatedNames = groupdByName.entrySet().stream().filter(entry -> entry.getValue().size() > 1).map(Map.Entry::getKey).toList();
            if (!duplicatedNames.isEmpty()) {
                throw new IllegalConfigurationException("'filterDefinitions' contains multiple items with the same names: " + duplicatedNames);
            }

            // Enforce post condition: Every filter referenced by a name is defined in the filterDefinitions
            Set<String> filterDefsByName = Optional.ofNullable(filterDefinitions).orElse(List.of()).stream().map(NamedFilterDefinition::name).collect(
                    Collectors.toSet());
            checkNamedFiltersAreDefined(filterDefsByName, defaultFilters, "defaultFilters");
            for (var virtualCluster : virtualClusters) {
                checkNamedFiltersAreDefined(filterDefsByName, virtualCluster.filters(), "virtualClusters." + virtualCluster.name() + ".filters");
            }

            checkAllNamedFilterAreUsed(filterDefinitions, virtualClusters, defaultFilters);
        }
    }

    private static void checkNamedFiltersAreDefined(Set<String> filterDefsByName,
                                                    @Nullable List<String> filterNames,
                                                    String componentName) {
        var unknown = Optional.ofNullable(filterNames)
                .orElse(List.of())
                .stream()
                .filter(filterName -> !filterDefsByName.contains(filterName))
                .toList();
        if (!unknown.isEmpty()) {
            throw new IllegalConfigurationException("'" + componentName + "' references filters not defined in 'filterDefinitions': " + unknown);
        }
    }

    private void validateNoDuplicatedClusterNames(List<VirtualCluster> clusters) {
        var names = clusters.stream()
                .map(VirtualCluster::name)
                .map(name -> name.toLowerCase(Locale.ROOT))
                .toList();
        var duplicates = names.stream()
                .filter(i -> Collections.frequency(names, i) > 1)
                .collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalConfigurationException(
                    "Virtual cluster must be unique (case insensitive). The following virtual cluster names are duplicated: [%s]".formatted(
                            String.join(", ", duplicates)));
        }
    }

    private void checkAllNamedFilterAreUsed(List<NamedFilterDefinition> filterDefinitions, List<VirtualCluster> clusters, List<String> defaultFilters) {
        var defined = filterDefinitions.stream().map(NamedFilterDefinition::name).collect(Collectors.toCollection(HashSet::new));
        if (defaultFilters != null) {
            defaultFilters.forEach(defined::remove);
        }
        clusters.stream()
                .map(VirtualCluster::filters)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .forEach(defined::remove);
        if (!defined.isEmpty()) {
            throw new IllegalConfigurationException(
                    "'filterDefinitions' defines filters which are not used in 'defaultFilters' or in any virtual cluster's 'filters': " + defined);
        }
    }

    private static VirtualClusterModel toVirtualClusterModel(@NonNull VirtualCluster virtualCluster,
                                                             @NonNull PluginFactoryRegistry pfr,
                                                             @NonNull List<NamedFilterDefinition> filterDefinitions) {

        VirtualClusterModel virtualClusterModel = new VirtualClusterModel(virtualCluster.name(),
                virtualCluster.targetCluster(),
                virtualCluster.logNetwork(),
                virtualCluster.logFrames(),
                filterDefinitions);

        addGateways(pfr, virtualCluster.gateways(), virtualClusterModel);
        virtualClusterModel.logVirtualClusterSummary();

        return virtualClusterModel;
    }

    private static void addGateways(@NonNull PluginFactoryRegistry pfr, List<VirtualClusterGateway> gateways, VirtualClusterModel virtualClusterModel) {
        gateways.forEach(gateway -> {
            var config = gateway.clusterNetworkAddressConfigProvider().config();
            var networkAddress = createDeprecatedProvider(config, virtualClusterModel);
            var tls = gateway.tls();
            virtualClusterModel.addGateway(gateway.name(), networkAddress, tls);
        });
    }

    @NonNull
    @SuppressWarnings("removal")
    private static ClusterNetworkAddressConfigProvider createDeprecatedProvider(Object config, VirtualClusterModel virtualClusterModel) {
        // We avoid using the buildNetworkAddressProviderService in order to avoid the deprecation notice it will produce.
        if (config instanceof SniRoutingClusterNetworkAddressConfigProviderConfig sniConfig) {
            return new SniRoutingClusterNetworkAddressConfigProvider().build(sniConfig, virtualClusterModel);
        }
        else if (config instanceof RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig rangeConfig) {
            return new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider().build(rangeConfig, virtualClusterModel);
        }
        else {
            throw new IllegalStateException("unexpected provider config type : " + config.getClass().getName());
        }
    }

    public List<MicrometerDefinition> getMicrometer() {
        return micrometer() == null ? List.of() : micrometer();
    }

    public boolean isUseIoUring() {
        return useIoUring();
    }

    public @NonNull List<VirtualClusterModel> virtualClusterModel(PluginFactoryRegistry pfr) {
        var filterDefinitionsByName = Optional.ofNullable(this.filterDefinitions()).orElse(List.of())
                .stream()
                .collect(Collectors.toMap(NamedFilterDefinition::name, Function.identity()));

        return virtualClusters.stream()
                .map(virtualCluster -> {
                    List<NamedFilterDefinition> filterDefinitions = namedFilterDefinitionsForCluster(filterDefinitionsByName, virtualCluster);
                    return toVirtualClusterModel(virtualCluster, pfr, filterDefinitions);
                })
                .toList();
    }

    @NonNull
    private List<NamedFilterDefinition> namedFilterDefinitionsForCluster(Map<String, NamedFilterDefinition> filterDefinitionsByName,
                                                                         VirtualCluster virtualCluster) {
        List<NamedFilterDefinition> filterDefinitions;
        List<String> clusterFilters = virtualCluster.filters();
        if (clusterFilters != null) {
            filterDefinitions = resolveFilterNames(filterDefinitionsByName, clusterFilters);
        }
        else if (defaultFilters != null) {
            filterDefinitions = resolveFilterNames(filterDefinitionsByName, defaultFilters);
        }
        else {
            filterDefinitions = List.of();
        }
        return filterDefinitions;
    }

    @NonNull
    private List<NamedFilterDefinition> resolveFilterNames(Map<String, NamedFilterDefinition> filterDefinitionsByName, List<String> filterNames) {
        return filterNames.stream()
                // Note: filterDefinitionsByName.get() returns non-null because of constructor post condition
                .map(filterDefinitionsByName::get)
                .toList();
    }

    /**
     * Custom deserializer that handles the possibility that the virtualClusters node may contain a list.
     * This deserializer can be removed once the deprecated map support is removed.
     */
    static class VirtualClusterContainerDeserializer extends StdDeserializer<List<VirtualCluster>> {
        VirtualClusterContainerDeserializer() {
            super(TypeFactory.defaultInstance().constructParametricType(List.class, VirtualCluster.class));
        }

        @Override
        public List<VirtualCluster> deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            JsonNode node = jp.getCodec().readTree(jp);
            if (node instanceof ObjectNode clusterMap) {
                return convertDeprecatedMapToList(ctxt, clusterMap);
            }
            else {
                return ctxt.readTreeAsValue(node, getValueType(ctxt));
            }
        }

        private List<VirtualCluster> convertDeprecatedMapToList(DeserializationContext ctxt, ObjectNode clusterMap) throws IOException {
            LOGGER.warn("The 'virtualCluster' configuration property with a map as a value is deprecated and support be removed in a future release. "
                    + "Configurations should be updated to define 'virtualCluster' with a list objects, including a 'name' property.");
            var clusterArrays = new ArrayNode(ctxt.getNodeFactory());
            var clusterNames = clusterMap.fieldNames();
            clusterNames.forEachRemaining(clusterName -> {
                JsonNode value = clusterMap.get(clusterName);
                if (value instanceof ObjectNode cluster) {
                    var currentName = cluster.get("name");
                    if (currentName == null) {
                        cluster.set("name", new TextNode(clusterName));
                    }
                    else if (!currentName.asText().equals(clusterName)) {
                        throw new IllegalConfigurationException(
                                ("Inconsistent virtual cluster configuration. "
                                        + "Configuration property 'virtualClusters' refers to a map, but the key name '%s' is different to the value of the 'name' field '%s' in the value.")
                                        .formatted(
                                                clusterName, currentName.asText()));
                    }
                    clusterArrays.add(cluster);
                }
            });
            return ctxt.readTreeAsValue(clusterArrays, _valueType);
        }
    }

    /**
     * Custom deserializer that reports the use of the deprecated configuration property
     * names {@code adminHttp} and {@code host}.
     */
    static class AdminHttpDeprecationLoggingDeserializer extends StdDeserializer<ManagementConfiguration> {
        AdminHttpDeprecationLoggingDeserializer() {
            super(TypeFactory.defaultInstance().constructType(ManagementConfiguration.class));
        }

        @Override
        public ManagementConfiguration deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            JsonNode node = jp.getCodec().readTree(jp);
            if ("adminHttp".equals(jp.currentName())) {
                LOGGER.warn("The 'adminHttp' configuration property is deprecated and will be removed in a future release. "
                        + "Configurations should replace 'adminHttp' with 'management'.");
            }
            if (node.has("host")) {
                LOGGER.warn("The 'host' configuration property within the '{}' object  is deprecated and will be removed in a future release. "
                        + "Configurations should replace 'host' with 'bindAddress'.", jp.currentName());
            }
            return ctxt.readTreeAsValue(node, getValueType(ctxt));
        }
    }
}
