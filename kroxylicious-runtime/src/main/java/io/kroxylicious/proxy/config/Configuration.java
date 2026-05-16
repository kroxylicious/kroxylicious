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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.internal.routing.RouteDescriptor;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The root of the proxy configuration.
 * <br>
 *
 * @param management management configuration
 * @param targetClusters Named target cluster definitions, referenced by routes and virtual clusters.
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
@JsonPropertyOrder({ "management", "targetClusters", "filterDefinitions", "defaultFilters", "routerDefinitions", "virtualClusters", "micrometer", "useIoUring",
        "development", "network", "proxyProtocol" })
public record Configuration(
                            @Nullable ManagementConfiguration management,
                            @Nullable List<TargetClusterDefinition> targetClusters,
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
        Set<String> targetClusterNames = validateTargetClusterDefinitions(targetClusters);

        // Enforce post condition: filterDefinitions have a unique name
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

    private static Set<String> validateTargetClusterDefinitions(@Nullable List<TargetClusterDefinition> targetClusters) {
        if (targetClusters == null || targetClusters.isEmpty()) {
            return Set.of();
        }
        var names = targetClusters.stream().map(TargetClusterDefinition::name).toList();
        var duplicates = names.stream()
                .filter(n -> Collections.frequency(names, n) > 1)
                .collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalConfigurationException(
                    "'targetClusters' contains duplicate names: " + duplicates);
        }
        return new HashSet<>(names);
    }

    private static void validateRouterDefinitions(@Nullable List<RouterDefinition> routerDefinitions,
                                                  Set<String> targetClusterNames,
                                                  Set<String> filterDefsByName) {
        if (routerDefinitions == null || routerDefinitions.isEmpty()) {
            return;
        }
        var routerNames = routerDefinitions.stream().map(RouterDefinition::name).toList();
        var duplicates = routerNames.stream()
                .filter(n -> Collections.frequency(routerNames, n) > 1)
                .collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalConfigurationException(
                    "'routerDefinitions' contains duplicate names: " + duplicates);
        }

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
                                                      Map<String, NamedFilterDefinition> filterDefinitionsByName,
                                                      PluginFactoryRegistry pfr) {
        TargetCluster resolvedTargetCluster = resolveTargetCluster(virtualCluster);
        Map<String, RouteDescriptor> routeDescriptors = resolveRouteDescriptors(
                virtualCluster, filterDefinitionsByName);

        VirtualClusterModel virtualClusterModel = new VirtualClusterModel(virtualCluster.name(),
                resolvedTargetCluster,
                virtualCluster.logNetwork(),
                virtualCluster.logFrames(),
                filterDefinitions,
                virtualCluster.topicNameCacheConfig(),
                virtualCluster.subjectBuilder(),
                virtualCluster.effectiveDrainTimeout(),
                pfr,
                virtualCluster.router(),
                routeDescriptors);

        addGateways(virtualCluster.gateways(), virtualClusterModel);
        virtualClusterModel.logVirtualClusterSummary();

        return virtualClusterModel;
    }

    @Nullable
    private Map<String, RouteDescriptor> resolveRouteDescriptors(
                                                                 VirtualCluster virtualCluster,
                                                                 Map<String, NamedFilterDefinition> filterDefinitionsByName) {
        if (virtualCluster.router() == null || routerDefinitions == null) {
            return null;
        }
        RouterDefinition rd = routerDefinitions.stream()
                .filter(r -> r.name().equals(virtualCluster.router()))
                .findFirst()
                .orElse(null);
        if (rd == null) {
            return null;
        }
        return rd.routes().stream()
                .collect(Collectors.toMap(
                        RouteDefinition::name,
                        route -> {
                            TargetCluster tc = route.targetCluster() != null
                                    ? resolveNamedTargetCluster(route.targetCluster())
                                    : null;
                            List<NamedFilterDefinition> routeFilters = route.filters() != null
                                    ? resolveFilterNames(filterDefinitionsByName, route.filters())
                                    : List.of();
                            return new RouteDescriptor(route.name(), tc, route.router(), routeFilters);
                        }));
    }

    @Nullable
    private TargetCluster resolveTargetCluster(VirtualCluster virtualCluster) {
        if (virtualCluster.targetCluster() != null) {
            return virtualCluster.targetCluster();
        }
        if (virtualCluster.namedTargetCluster() != null) {
            return Optional.ofNullable(targetClusters).orElse(List.of()).stream()
                    .filter(tc -> tc.name().equals(virtualCluster.namedTargetCluster()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalConfigurationException(
                            "Virtual cluster '" + virtualCluster.name() + "' references unknown target cluster '"
                                    + virtualCluster.namedTargetCluster() + "'"))
                    .toTargetCluster();
        }
        if (virtualCluster.router() != null) {
            return resolveRouterPrimaryTargetCluster(virtualCluster);
        }
        return null;
    }

    @Nullable
    private TargetCluster resolveRouterPrimaryTargetCluster(VirtualCluster virtualCluster) {
        if (routerDefinitions == null) {
            return null;
        }
        return routerDefinitions.stream()
                .filter(rd -> rd.name().equals(virtualCluster.router()))
                .findFirst()
                .flatMap(rd -> rd.routes().stream()
                        .filter(route -> route.targetCluster() != null)
                        .findFirst()
                        .map(route -> resolveNamedTargetCluster(route.targetCluster())))
                .orElse(null);
    }

    @Nullable
    private TargetCluster resolveNamedTargetCluster(String name) {
        return Optional.ofNullable(targetClusters).orElse(List.of()).stream()
                .filter(tc -> tc.name().equals(name))
                .findFirst()
                .map(TargetClusterDefinition::toTargetCluster)
                .orElse(null);
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
        var filterDefinitionsByName = Optional.ofNullable(this.filterDefinitions()).orElse(List.of())
                .stream()
                .collect(Collectors.toMap(NamedFilterDefinition::name, Function.identity()));

        return virtualClusters.stream()
                .map(virtualCluster -> {
                    List<NamedFilterDefinition> filterDefinitions = namedFilterDefinitionsForCluster(
                            filterDefinitionsByName, virtualCluster);
                    return toVirtualClusterModel(virtualCluster, filterDefinitions,
                            filterDefinitionsByName, pfr);
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
