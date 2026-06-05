/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
 * @param clusterDefinitions Named target cluster definitions, referenced by virtual clusters and routes.
 * @param filterDefinitions A list of named filter definitions (names must be unique)
 * @param defaultFilters The names of the {@link #filterDefinitions()} to be use when a {@link VirtualCluster} doesn't specify its own {@link VirtualCluster#filters()}.
 * @param routerDefinitions Named router definitions.
 * @param virtualClusters The virtual clusters
 * @param micrometer The micrometer config
 * @param useIoUring true to use iouring
 * @param development Development options
 * @param network Controls aspects of network configuration for the proxy.
 * @param proxyProtocol PROXY protocol configuration.
 */
@JsonPropertyOrder({ "management", "clusterDefinitions", "filterDefinitions", "defaultFilters", "routerDefinitions", "virtualClusters", "micrometer", "useIoUring",
        "development", "network", "proxyProtocol" })
public record Configuration(
                            @Nullable ManagementConfiguration management,
                            @Nullable List<ClusterDefinition> clusterDefinitions,
                            @Nullable List<NamedFilterDefinition> filterDefinitions,
                            @Nullable List<String> defaultFilters,
                            @Nullable List<RouterDefinition> routerDefinitions,
                            @JsonProperty(required = true) List<VirtualCluster> virtualClusters,
                            @Nullable List<MicrometerDefinition> micrometer,
                            boolean useIoUring,
                            Optional<Map<String, Object>> development,
                            @Nullable NetworkDefinition network,
                            @Nullable ProxyProtocolConfig proxyProtocol) {

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
        Set<String> targetClusterNames = validateClusterDefinitions(clusterDefinitions);
        validateVirtualClusterNamedTargets(virtualClusters, targetClusterNames);

        Set<String> filterDefsByName = Set.of();
        if (filterDefinitions != null) {
            Map<String, List<NamedFilterDefinition>> groupdByName = filterDefinitions.stream().collect(Collectors.groupingBy(NamedFilterDefinition::name));
            var duplicatedNames = groupdByName.entrySet().stream().filter(entry -> entry.getValue().size() > 1).map(Map.Entry::getKey).toList();
            if (!duplicatedNames.isEmpty()) {
                throw new IllegalConfigurationException("'filterDefinitions' contains multiple items with the same names: " + duplicatedNames);
            }

            filterDefsByName = filterDefinitions.stream().map(NamedFilterDefinition::name).collect(Collectors.toSet());
            checkNamedFiltersAreDefined(filterDefsByName, defaultFilters, "defaultFilters");
            for (var virtualCluster : virtualClusters) {
                checkNamedFiltersAreDefined(filterDefsByName, virtualCluster.filters(), "virtualClusters." + virtualCluster.name() + ".filters");
            }
        }

        validateRouterDefinitions(routerDefinitions, targetClusterNames, filterDefsByName);
        validateVirtualClusterReceivers(virtualClusters, targetClusterNames, routerDefinitions);

        if (filterDefinitions != null) {
            checkAllNamedFilterAreUsed(filterDefinitions, virtualClusters, defaultFilters, routerDefinitions);
        }
    }

    /**
     * Validates that cluster definition names are unique and returns the set of names.
     */
    private static Set<String> validateClusterDefinitions(@Nullable List<ClusterDefinition> clusterDefinitions) {
        if (clusterDefinitions == null || clusterDefinitions.isEmpty()) {
            return Set.of();
        }
        return validateUniqueNames(clusterDefinitions, ClusterDefinition::name, "clusterDefinitions");
    }

    /**
     * Validates that router definition names are unique, that all route filter references
     * are defined, and that the router graph is a valid DAG.
     */
    private static void validateRouterDefinitions(@Nullable List<RouterDefinition> routerDefinitions,
                                                  Set<String> targetClusterNames,
                                                  Set<String> filterDefsByName) {
        if (routerDefinitions == null || routerDefinitions.isEmpty()) {
            return;
        }
        validateUniqueNames(routerDefinitions, RouterDefinition::name, "routerDefinitions");

        for (var router : routerDefinitions) {
            for (var route : router.routes()) {
                if (route.filters() != null) {
                    checkNamedFiltersAreDefined(filterDefsByName, route.filters(),
                            "routerDefinitions." + router.name() + ".routes." + route.name() + ".filters");
                }
            }
        }

        RouterGraphValidator.validate(routerDefinitions, targetClusterNames);
    }

    /**
     * Validates that named target cluster references in virtual clusters resolve to known cluster definitions.
     */
    private static void validateVirtualClusterNamedTargets(List<VirtualCluster> virtualClusters,
                                                           Set<String> targetClusterNames) {
        for (var vc : virtualClusters) {
            if (vc.namedTargetCluster() != null && !targetClusterNames.contains(vc.namedTargetCluster())) {
                throw new IllegalConfigurationException(
                        "Virtual cluster '" + vc.name() + "' references unknown target cluster '" + vc.namedTargetCluster() + "'");
            }
        }
    }

    /**
     * Validates that virtual cluster target references (both cluster and router) resolve
     * to known definitions.
     */
    private static void validateVirtualClusterReceivers(List<VirtualCluster> virtualClusters,
                                                        Set<String> targetClusterNames,
                                                        @Nullable List<RouterDefinition> routerDefinitions) {
        Set<String> routerNames = Optional.ofNullable(routerDefinitions).orElse(List.of()).stream()
                .map(RouterDefinition::name)
                .collect(Collectors.toSet());

        for (var vc : virtualClusters) {
            if (vc.namedTargetCluster() != null && !targetClusterNames.contains(vc.namedTargetCluster())) {
                throw new IllegalConfigurationException(
                        "Virtual cluster '" + vc.name() + "' references unknown target cluster '" + vc.namedTargetCluster() + "'");
            }
            if (vc.router() != null && !routerNames.contains(vc.router())) {
                throw new IllegalConfigurationException(
                        "Virtual cluster '" + vc.name() + "' references unknown router '" + vc.router() + "'");
            }
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

    /**
     * Validates that virtual cluster names are unique (case-insensitive comparison).
     */
    private void validateNoDuplicatedClusterNames(List<VirtualCluster> clusters) {
        Set<String> seen = new HashSet<>();
        Set<String> duplicates = new LinkedHashSet<>();
        for (var vc : clusters) {
            if (!seen.add(vc.name().toLowerCase(Locale.ROOT))) {
                duplicates.add(vc.name().toLowerCase(Locale.ROOT));
            }
        }
        if (!duplicates.isEmpty()) {
            throw new IllegalConfigurationException(
                    "Virtual cluster must be unique (case insensitive). The following virtual cluster names are duplicated: [%s]".formatted(
                            String.join(", ", duplicates)));
        }
    }

    private static <T> Set<String> validateUniqueNames(List<T> items,
                                                       Function<T, String> nameExtractor,
                                                       String context) {
        Set<String> seen = new HashSet<>();
        Set<String> duplicates = new LinkedHashSet<>();
        for (T item : items) {
            if (!seen.add(nameExtractor.apply(item))) {
                duplicates.add(nameExtractor.apply(item));
            }
        }
        if (!duplicates.isEmpty()) {
            throw new IllegalConfigurationException(
                    "'" + context + "' contains duplicate names: " + duplicates);
        }
        return seen;
    }

    private static void checkAllNamedFilterAreUsed(List<NamedFilterDefinition> filterDefinitions,
                                                   List<VirtualCluster> clusters,
                                                   @Nullable List<String> defaultFilters,
                                                   @Nullable List<RouterDefinition> routerDefinitions) {
        var defined = filterDefinitions.stream().map(NamedFilterDefinition::name).collect(Collectors.toCollection(HashSet::new));
        if (defaultFilters != null) {
            defaultFilters.forEach(defined::remove);
        }
        clusters.stream()
                .map(VirtualCluster::filters)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .forEach(defined::remove);
        if (routerDefinitions != null) {
            routerDefinitions.stream()
                    .flatMap(r -> r.routes().stream())
                    .map(RouteDefinition::filters)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .forEach(defined::remove);
        }
        if (!defined.isEmpty()) {
            throw new IllegalConfigurationException(
                    "'filterDefinitions' defines filters which are not used in 'defaultFilters', "
                            + "in any virtual cluster's 'filters', or in any route's 'filters': " + defined);
        }
    }

    private VirtualClusterModel toVirtualClusterModel(VirtualCluster virtualCluster,
                                                      List<NamedFilterDefinition> filterDefinitions,
                                                      PluginFactoryRegistry pfr) {
        TargetCluster resolvedTargetCluster = resolveTargetCluster(virtualCluster);

        VirtualClusterModel virtualClusterModel = new VirtualClusterModel(virtualCluster.name(),
                resolvedTargetCluster,
                virtualCluster.logNetwork(),
                virtualCluster.logFrames(),
                filterDefinitions,
                virtualCluster.topicNameCacheConfig(),
                virtualCluster.subjectBuilder(),
                virtualCluster.effectiveDrainTimeout(),
                pfr);

        addGateways(virtualCluster.gateways(), virtualClusterModel);
        virtualClusterModel.logVirtualClusterSummary();

        return virtualClusterModel;
    }

    private TargetCluster resolveTargetCluster(VirtualCluster virtualCluster) {
        if (virtualCluster.targetCluster() != null) {
            return virtualCluster.targetCluster();
        }
        if (virtualCluster.namedTargetCluster() != null) {
            return resolveNamedTargetCluster(virtualCluster.namedTargetCluster(), virtualCluster.name());
        }
        throw new IllegalConfigurationException(
                "Virtual cluster '" + virtualCluster.name() + "' has no resolvable target cluster");
    }

    private TargetCluster resolveNamedTargetCluster(String clusterName, String virtualClusterName) {
        return Optional.ofNullable(clusterDefinitions).orElse(List.of()).stream()
                .filter(cd -> cd.name().equals(clusterName))
                .findFirst()
                .orElseThrow(() -> new IllegalConfigurationException(
                        "Virtual cluster '" + virtualClusterName + "' references unknown target cluster '" + clusterName + "'"))
                .toTargetCluster();
    }

    private static void addGateways(List<VirtualClusterGateway> gateways, VirtualClusterModel virtualClusterModel) {
        gateways.forEach(gateway -> {
            try {
                var nodeIdentificationStrategy = gateway.buildNodeIdentificationStrategy(virtualClusterModel.getClusterName());
                var tls = gateway.tls();
                virtualClusterModel.addGateway(gateway.name(), nodeIdentificationStrategy, tls);
            }
            catch (SniHostIdentifiesNodeIdentificationStrategy.UnresolvedHostException e) {
                LOGGER.atWarn()
                        .addKeyValue("gateway", gateway.name())
                        .log("Not adding gateway due to unresolved host (associated OpenShift Route may not be ready yet)");
            }
        });
    }

    public List<MicrometerDefinition> getMicrometer() {
        return micrometer() == null ? List.of() : micrometer();
    }

    public boolean isUseIoUring() {
        return useIoUring();
    }

    public ProxyProtocolMode proxyProtocolMode() {
        return proxyProtocol != null ? proxyProtocol.mode() : ProxyProtocolMode.DISABLED;
    }

    public List<VirtualClusterModel> virtualClusterModel(PluginFactoryRegistry pfr) {
        rejectUnsupportedRoutingConfig();
        var filterDefinitionsByName = buildFilterDefinitionsByName();

        return virtualClusters.stream()
                .map(virtualCluster -> {
                    List<NamedFilterDefinition> filterDefinitions = namedFilterDefinitionsForCluster(filterDefinitionsByName, virtualCluster);
                    return toVirtualClusterModel(virtualCluster, filterDefinitions, pfr);
                })
                .toList();
    }

    /**
     * Builds the {@link VirtualClusterModel} for a single virtual cluster by name. Unlike
     * {@link #virtualClusterModel(PluginFactoryRegistry)}, this avoids constructing models
     * (and therefore avoids running each filter's {@code initialize()}) for any virtual cluster
     * other than the one requested. Used by {@code OperationsPlanner} so a reconfigure that
     * targets one cluster doesn't orphan-initialise the filters of unrelated clusters.
     *
     * @throws IllegalArgumentException if no virtual cluster with that name exists in this configuration
     */
    public VirtualClusterModel virtualClusterModel(PluginFactoryRegistry pfr, String clusterName) {
        rejectUnsupportedRoutingConfig();
        var filterDefinitionsByName = buildFilterDefinitionsByName();

        return virtualClusters.stream()
                .filter(virtualCluster -> virtualCluster.name().equals(clusterName))
                .findFirst()
                .map(virtualCluster -> {
                    List<NamedFilterDefinition> filterDefinitions = namedFilterDefinitionsForCluster(filterDefinitionsByName, virtualCluster);
                    return toVirtualClusterModel(virtualCluster, filterDefinitions, pfr);
                })
                .orElseThrow(() -> new IllegalArgumentException("No virtual cluster named '" + clusterName + "' in this configuration"));
    }

    private void rejectUnsupportedRoutingConfig() {
        if (routerDefinitions != null && !routerDefinitions.isEmpty()) {
            throw new IllegalConfigurationException(
                    "Routing is not yet supported in this version. Remove 'routerDefinitions' from configuration.");
        }
        for (var vc : virtualClusters) {
            if (vc.router() != null) {
                throw new IllegalConfigurationException(
                        "Routing is not yet supported in this version. Virtual cluster '"
                                + vc.name() + "' uses a router target, which is not yet implemented.");
            }
        }
    }

    private Map<String, NamedFilterDefinition> buildFilterDefinitionsByName() {
        return Optional.ofNullable(this.filterDefinitions()).orElse(List.of())
                .stream()
                .collect(Collectors.toMap(NamedFilterDefinition::name, Function.identity()));
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
