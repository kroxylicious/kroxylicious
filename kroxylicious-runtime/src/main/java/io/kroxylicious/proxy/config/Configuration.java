/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The root of the proxy configuration.
 * <br>
 *
 * @param management management configuration
 * @param filterDefinitions A list of named filter definitions (names must be unique)
 * @param defaultFilters The names of the {@link #filterDefinitions()} to be use when a {@link VirtualCluster} doesn't specify its own {@link VirtualCluster#filters()}.
 * @param virtualClusters The virtual clusters
 * @param micrometer The micrometer config
 * @param useIoUring true to use iouring
 * @param development Development options
 * @param network Controls aspects of network configuration for the proxy.
 */
@JsonPropertyOrder({ "management", "filterDefinitions", "defaultFilters", "virtualClusters", "micrometer", "useIoUring", "development", "network" })
public record Configuration(
                            @Nullable ManagementConfiguration management,
                            @Nullable List<NamedFilterDefinition> filterDefinitions,
                            @Nullable List<String> defaultFilters,
                            @JsonProperty(required = true) List<VirtualCluster> virtualClusters,
                            @Nullable List<MicrometerDefinition> micrometer,
                            @Deprecated boolean useIoUring,
                            Optional<Map<String, Object>> development,
                            @Nullable NetworkDefinition network) {

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

    @JsonIgnore
    public NetworkDefinition networkDefinition() {
        return Objects.nonNull(network) ? network : NetworkDefinition.defaultNetworkDefinition();
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

    private static VirtualClusterModel toVirtualClusterModel(VirtualCluster virtualCluster,
                                                             List<NamedFilterDefinition> filterDefinitions) {

        VirtualClusterModel virtualClusterModel = new VirtualClusterModel(virtualCluster.name(),
                virtualCluster.targetCluster(),
                virtualCluster.logNetwork(),
                virtualCluster.logFrames(),
                filterDefinitions);

        addGateways(virtualCluster.gateways(), virtualClusterModel);
        virtualClusterModel.logVirtualClusterSummary();

        return virtualClusterModel;
    }

    private static void addGateways(List<VirtualClusterGateway> gateways, VirtualClusterModel virtualClusterModel) {
        gateways.forEach(gateway -> {
            var nodeIdentificationStrategy = gateway.buildNodeIdentificationStrategy(virtualClusterModel.getClusterName());
            var tls = gateway.tls();
            virtualClusterModel.addGateway(gateway.name(), nodeIdentificationStrategy, tls);
        });
    }

    public List<MicrometerDefinition> getMicrometer() {
        return micrometer() == null ? List.of() : micrometer();
    }

    public boolean isUseIoUring() {
        return useIoUring();
    }

    public List<VirtualClusterModel> virtualClusterModel(PluginFactoryRegistry pfr) {
        var filterDefinitionsByName = Optional.ofNullable(this.filterDefinitions()).orElse(List.of())
                .stream()
                .collect(Collectors.toMap(NamedFilterDefinition::name, Function.identity()));

        return virtualClusters.stream()
                .map(virtualCluster -> {
                    List<NamedFilterDefinition> filterDefinitions = namedFilterDefinitionsForCluster(filterDefinitionsByName, virtualCluster);
                    return toVirtualClusterModel(virtualCluster, filterDefinitions);
                })
                .toList();
    }

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

    private List<NamedFilterDefinition> resolveFilterNames(Map<String, NamedFilterDefinition> filterDefinitionsByName, List<String> filterNames) {
        return filterNames.stream()
                // Note: filterDefinitionsByName.get() returns non-null because of constructor post condition
                .map(filterDefinitionsByName::get)
                .toList();
    }

}
