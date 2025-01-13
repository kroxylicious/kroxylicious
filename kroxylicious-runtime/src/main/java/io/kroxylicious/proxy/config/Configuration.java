/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The root of the proxy configuration.
 */
@JsonPropertyOrder({ "adminHttp", "filterDefinitions", "defaultFilters", "virtualClusters", "filters", "micrometer", "useIoUring", "development" })
public record Configuration(
                            @Nullable AdminHttpConfiguration adminHttp,
                            @Nullable List<NamedFilterDefinition> filterDefinitions,
                            @Nullable List<String> defaultFilters,
                            Map<String, VirtualCluster> virtualClusters,
                            @Deprecated @Nullable List<FilterDefinition> filters,
                            List<MicrometerDefinition> micrometer,
                            boolean useIoUring,
                            @NonNull Optional<Map<String, Object>> development) {

    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

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

    /**
     * Specifying {@code filters} is deprecated.
     * Use the {@link Configuration#Configuration(AdminHttpConfiguration, List, List, Map, List, boolean, Optional)} constructor instead.
     * @param adminHttp
     * @param filterDefinitions A list of named filter definitions (names must be unique)
     * @param defaultFilters The names of the {@link #filterDefinitions()} to be use when a {@link VirtualCluster} doesn't specify its own {@link VirtualCluster#filters()}.
     * @param virtualClusters The virtual clusters
     * @param filters Deprecated. The filter definitions to be used for all virtual clusters. Can only be specified if {@link #filterDefinitions()} is null.
     * @param micrometer The micrometer config
     * @param useIoUring true to use iouring
     * @param development Development options
     */
    @Deprecated(since = "0.10.0", forRemoval = true)
    @JsonCreator
    public Configuration {
        Objects.requireNonNull(development);
        // Enforce post condition: filters and filterDefinitions are not both set
        if (filters != null && filterDefinitions != null) {
            throw new IllegalConfigurationException("'filters' and 'filterDefinitions' can't both be set");
        }

        // Enforce post condition: filterDefinitions have a unique name
        if (filterDefinitions != null) {
            Map<String, List<NamedFilterDefinition>> groupdByName = filterDefinitions.stream().collect(Collectors.groupingBy(NamedFilterDefinition::name));
            var duplicatedNames = groupdByName.entrySet().stream().filter(entry -> entry.getValue().size() > 1).map(Map.Entry::getKey).toList();
            if (!duplicatedNames.isEmpty()) {
                throw new IllegalConfigurationException("'filterDefinitions' contains multiple items with the same names: " + duplicatedNames);
            }
        }

        // Enforce post condition: Every filter referenced by a name is defined in the filterDefinitions
        Set<String> filterDefsByName = Optional.ofNullable(filterDefinitions).orElse(List.of()).stream().map(NamedFilterDefinition::name).collect(
                Collectors.toSet());
        checkNamedFiltersAreDefined(filterDefsByName, defaultFilters, "defaultFilters");
        if (virtualClusters != null) {
            for (var entry : virtualClusters.entrySet()) {
                var virtualClusterName = entry.getKey();
                var virtualCluster = entry.getValue();
                checkNamedFiltersAreDefined(filterDefsByName, virtualCluster.filters(), "virtualClusters." + virtualClusterName + ".filterRefs");
            }
        }

        // Every filter defined in the filterDefinitions is used somewhere
        if (filterDefinitions != null) {
            var defined = filterDefinitions.stream().map(NamedFilterDefinition::name).collect(Collectors.toCollection(HashSet::new));
            if (defaultFilters != null) {
                defaultFilters.forEach(defined::remove);
            }
            if (virtualClusters != null) {
                virtualClusters.values().stream()
                        .map(VirtualCluster::filters)
                        .filter(Objects::nonNull)
                        .flatMap(Collection::stream)
                        .forEach(defined::remove);
            }
            if (!defined.isEmpty()) {
                throw new IllegalConfigurationException(
                        "'filterDefinitions' defines filters which are not used in 'defaultFilters' or in any virtual cluster's 'filters': " + defined);
            }
        }

        if (filters != null && virtualClusters != null && virtualClusters.values().stream()
                .map(VirtualCluster::filters)
                .anyMatch(Objects::nonNull)) {
            throw new IllegalConfigurationException(
                    "'filters' cannot be specified on a virtual cluster when 'filters' is defined at the top level.");
        }

        if (filters != null) {
            LOGGER.warn("The 'filters' configuration property is deprecated and will be removed in a future release. "
                    + "Configurations should be updated to use 'filterDefinitions' and 'defaultFilters'.");
        }
    }

    /**
     * @deprecated This constructor is currently retained to be source compatible the call sites that are passing the deprecated `filters` parameter.
     * Replaced by {@link #Configuration(AdminHttpConfiguration, List, List, Map, List, boolean, Optional)}.
     */
    @Deprecated(since = "0.10.0", forRemoval = true)
    public Configuration(
                         @Nullable AdminHttpConfiguration adminHttp,
                         Map<String, VirtualCluster> virtualClusters,
                         @Nullable List<FilterDefinition> filters,
                         List<MicrometerDefinition> micrometer,
                         boolean useIoUring,
                         @NonNull Optional<Map<String, Object>> development) {
        this(adminHttp, null, null, virtualClusters, filters, micrometer, useIoUring, development);
    }

    /**
     * This constructor uses the new style `defaultFilters` and `filterDefinitions` parameters instead of the deprecated `filters`.
     */
    public Configuration(
                         @Nullable AdminHttpConfiguration adminHttp, @Nullable List<NamedFilterDefinition> filterDefinitions,
                         @Nullable List<String> defaultFilters,
                         Map<String, VirtualCluster> virtualClusters,
                         List<MicrometerDefinition> micrometer, boolean useIoUring,
                         @NonNull Optional<Map<String, Object>> development) {
        this(adminHttp, filterDefinitions, defaultFilters, virtualClusters, null, micrometer, useIoUring, development);
    }

    public @Nullable AdminHttpConfiguration adminHttpConfig() {
        return adminHttp();
    }

    public List<MicrometerDefinition> getMicrometer() {
        return micrometer() == null ? List.of() : micrometer();
    }

    public boolean isUseIoUring() {
        return useIoUring();
    }

    /**
     * @deprecated This will be removed when support for {@code filters} is removed.
     * @return NamedFilterDefinition for all the filters defined in the configuration, generating names if the config specified {@link #filters()}.
     */
    @Deprecated(since = "0.10.0", forRemoval = true)
    public @NonNull List<NamedFilterDefinition> toNamedFilterDefinitions() {
        if (filterDefinitions != null) {
            return filterDefinitions;
        }
        else {
            return toNamedFilterDefinitions(filters != null ? filters : List.of());
        }
    }

    /**
     * Generate named filters for the given anonymous filters.
     * The filter's type is used as the name, unless this is ambiguous, in which case the type is disambiguated
     * using a suffix based on the index of the filter within the list.
     * @param filters The anonymous filters.
     * @return The named filters.
     */
    @NonNull
    public static List<NamedFilterDefinition> toNamedFilterDefinitions(List<FilterDefinition> filters) {
        var multipleTypes = filters.stream()
                .collect(Collectors.groupingBy(FilterDefinition::type))
                .entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        List<NamedFilterDefinition> filterDefinitions;
        filterDefinitions = new ArrayList<>();
        HashMap<String, Integer> typeCounts = new HashMap<>(1 + (int) (multipleTypes.size() / 0.75f));
        for (FilterDefinition anonymousFilter : Optional.ofNullable(filters).orElse(List.of())) {
            String filterName = anonymousFilter.type();
            if (multipleTypes.contains(anonymousFilter.type())) {
                int count = typeCounts.compute(anonymousFilter.type(), (type, typeCount) -> typeCount == null ? 0 : ++typeCount);
                filterName += "-" + count;
            }
            filterDefinitions.add(new NamedFilterDefinition(filterName, anonymousFilter.type(), anonymousFilter.config()));
        }
        return filterDefinitions;
    }

    public @NonNull List<io.kroxylicious.proxy.model.VirtualCluster> virtualClusterModel(PluginFactoryRegistry pfr) {
        var filterDefinitionsByName = Optional.ofNullable(this.filterDefinitions()).orElse(List.of())
                .stream()
                .collect(Collectors.toMap(NamedFilterDefinition::name, Function.identity()));

        return virtualClusters.entrySet().stream()
                .map(entry -> {
                    VirtualCluster virtualCluster = entry.getValue();
                    List<NamedFilterDefinition> filterDefinitions = namedFilterDefinitionsForCluster(filterDefinitionsByName, virtualCluster);
                    return virtualCluster.toVirtualClusterModel(pfr, filterDefinitions, entry.getKey());
                })
                .toList();
    }

    @NonNull
    private List<NamedFilterDefinition> namedFilterDefinitionsForCluster(Map<String, NamedFilterDefinition> filterDefinitionsByName,
                                                                         VirtualCluster virtualCluster) {
        List<NamedFilterDefinition> filterDefinitions;
        List<String> clusterFilterRefs = virtualCluster.filters();
        if (clusterFilterRefs != null) {
            filterDefinitions = resolveFilterNames(filterDefinitionsByName, clusterFilterRefs);
        }
        else if (defaultFilters != null) {
            filterDefinitions = resolveFilterNames(filterDefinitionsByName, defaultFilters);
        }
        else {
            filterDefinitions = toNamedFilterDefinitions(filters != null ? filters : List.of());
        }
        return filterDefinitions;
    }

    @NonNull
    private List<NamedFilterDefinition> resolveFilterNames(Map<String, NamedFilterDefinition> filterDefinitionsByName, List<String> filterNames) {
        return filterNames.stream()
                // Note: filterDefinitionsByName.get() returns non-null because of constructor post cindition
                .map(filterDefinitionsByName::get)
                .toList();
    }
}
