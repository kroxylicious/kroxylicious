/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;

/**
 * Detects potential for port conflicts arising between virtual cluster configurations.
 */
public class PortConflictDetector {

    private static final String ANY = "<any>";

    private enum BindingScope {
        SHARED,
        EXCLUSIVE
    }

    /**
     * Validates the configuration throwing an exception if a conflict is detected.
     *
     * @param virtualClusterMap map of virtual clusters.
     */
    public void validate(Map<String, VirtualCluster> virtualClusterMap) {

        Set<String> seenVirtualClusters = new HashSet<>();
        Set<Integer> anyInterfaceExclusivePorts = new HashSet<>();
        Map<String, Set<Integer>> specificInterfaceExclusivePorts = new HashMap<>();

        Map<Integer, Boolean> anyInterfaceSharedPorts = new HashMap<>();
        Map<String, Map<Integer, Boolean>> specificInterfaceSharedPorts = new HashMap<>();

        virtualClusterMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> {
            var name = e.getKey();
            var cluster = e.getValue();
            var sharedPorts = cluster.getSharedPorts();
            var exclusivePorts = cluster.getExclusivePorts();
            var specificInterface = cluster.getBindAddress().isPresent();

            // check exclusive ports

            // check whether this virtual cluster's configuration would produce conflicts on <any> interface (0.0.0.0)
            var conflicts = getSortedPortConflicts(anyInterfaceExclusivePorts, exclusivePorts);
            if (!conflicts.isEmpty()) {
                throw buildPortConflictException(name, seenVirtualClusters, conflicts,
                        getBindingAddressAsString(cluster), BindingScope.EXCLUSIVE,
                        ANY, BindingScope.EXCLUSIVE);
            }

            if (specificInterface) {
                // binding to a specific interface

                // check whether one or more of the proposed exclusive ports are already bound on the same interface
                var targetBindingAddress = cluster.getBindAddress().get();
                var existing = specificInterfaceExclusivePorts.get(cluster.getBindAddress().get());
                if (existing != null) {
                    var specificInterfaceConflicts = getSortedPortConflicts(existing, exclusivePorts);
                    if (!specificInterfaceConflicts.isEmpty()) {
                        throw buildPortConflictException(name, seenVirtualClusters, specificInterfaceConflicts,
                                targetBindingAddress, BindingScope.EXCLUSIVE,
                                targetBindingAddress, BindingScope.EXCLUSIVE);
                    }
                }
            }
            else {
                // binding to any

                // check whether one or more of the proposed exclusive ports are already bound to any other interface
                specificInterfaceExclusivePorts.forEach((bindingAddress, ports) -> {
                    var existing = specificInterfaceExclusivePorts.get(bindingAddress);

                    var specificInterfaceConflicts = getSortedPortConflicts(existing, exclusivePorts);
                    if (!specificInterfaceConflicts.isEmpty()) {
                        throw buildPortConflictException(name, seenVirtualClusters, specificInterfaceConflicts,
                                ANY, BindingScope.EXCLUSIVE,
                                bindingAddress, BindingScope.EXCLUSIVE);
                    }
                });
            }

            // check shared ports

            if (specificInterface) {
                // binding to a specific interface

                var targetBindingAddress = cluster.getBindAddress().get();

                // check whether one or more of the proposed shared ports are already bound to the <any> interface
                var existing = getSortedPortConflicts(anyInterfaceSharedPorts.keySet(), sharedPorts);
                if (!existing.isEmpty()) {
                    throw buildPortConflictException(name, seenVirtualClusters, existing,
                            targetBindingAddress, BindingScope.SHARED,
                            ANY, BindingScope.SHARED);
                }

                // check whether one or more of the proposed shared ports are already used exclusively
                var existingExclusive = specificInterfaceExclusivePorts.get(targetBindingAddress);
                if (existingExclusive != null) {
                    var specificInterfaceConflicts = getSortedPortConflicts(existingExclusive, sharedPorts);
                    if (!specificInterfaceConflicts.isEmpty()) {
                        throw buildPortConflictException(name, seenVirtualClusters, specificInterfaceConflicts,
                                targetBindingAddress, BindingScope.SHARED,
                                targetBindingAddress, BindingScope.EXCLUSIVE);
                    }
                }

                var existingShared = specificInterfaceSharedPorts.get(targetBindingAddress);
                if (existingShared != null) {
                    // check whether one or more of the proposed exclusive ports are already shared
                    var specificInterfaceConflicts = getSortedPortConflicts(existingShared.keySet(), exclusivePorts);
                    if (!specificInterfaceConflicts.isEmpty()) {
                        throw buildPortConflictException(name, seenVirtualClusters, specificInterfaceConflicts,
                                targetBindingAddress, BindingScope.EXCLUSIVE,
                                targetBindingAddress, BindingScope.SHARED);
                    }

                    sharedPorts.forEach(p -> {
                        var tls = existingShared.get(p);
                        if (tls != null && !tls.equals(cluster.isUseTls())) {
                            throw buildOverviewException(name, seenVirtualClusters, buildTlsConflictException(p, targetBindingAddress));
                        }
                    });
                }

            }
            else {
                // binding to any

                // check whether one or more of the proposed shared ports are already bound to other specific interface
                specificInterfaceSharedPorts.forEach((bindingAddress, ports) -> {
                    var existing = getSortedPortConflicts(ports.keySet(), sharedPorts);
                    if (!existing.isEmpty()) {
                        throw buildPortConflictException(name, seenVirtualClusters, existing,
                                bindingAddress, BindingScope.SHARED,
                                ANY, BindingScope.SHARED);
                    }
                });

                // check whether one or more of the proposed exclusive ports are already shared
                var existingExclusive = getSortedPortConflicts(anyInterfaceSharedPorts.keySet(), exclusivePorts);
                if (!existingExclusive.isEmpty()) {
                    throw buildPortConflictException(name, seenVirtualClusters, existingExclusive,
                            ANY, BindingScope.EXCLUSIVE,
                            ANY, BindingScope.SHARED);
                }

                // check whether one or more of the proposed shared ports are already exclusive
                var existingShared = getSortedPortConflicts(anyInterfaceExclusivePorts, sharedPorts);
                if (!existingShared.isEmpty()) {
                    throw buildPortConflictException(name, seenVirtualClusters, existingShared,
                            ANY, BindingScope.SHARED,
                            ANY, BindingScope.EXCLUSIVE);
                }

                sharedPorts.forEach(p -> {
                    var tls = anyInterfaceSharedPorts.get(p);
                    if (tls != null && !tls.equals(cluster.isUseTls())) {
                        throw buildOverviewException(name, seenVirtualClusters, buildTlsConflictException(p, ANY));
                    }
                });

            }

            seenVirtualClusters.add(name);
            if (cluster.getBindAddress().isEmpty()) {
                anyInterfaceExclusivePorts.addAll(exclusivePorts);
                sharedPorts.forEach(p -> anyInterfaceSharedPorts.put(p, cluster.isUseTls()));
            }
            else {
                specificInterfaceExclusivePorts.computeIfAbsent(cluster.getBindAddress().get(), (k) -> new HashSet<>()).addAll(exclusivePorts);
                var sharedMap = specificInterfaceSharedPorts.computeIfAbsent(cluster.getBindAddress().get(), (k) -> new HashMap<>());
                sharedPorts.forEach(p -> sharedMap.put(p, cluster.isUseTls()));
            }
        });
    }

    private List<Integer> getSortedPortConflicts(Set<Integer> ports, Set<Integer> candidates) {
        return ports.stream().filter(candidates::contains).sorted().collect(Collectors.toList());
    }

    private IllegalStateException buildPortConflictException(String virtualClusterName, Set<String> seenVirtualClusters, List<Integer> conflicts,
                                                             String proposedBindingInterface, BindingScope proposedScope,
                                                             String existingBindingInterface, BindingScope existingBindingScope) {
        var portConflicts = conflicts.stream().map(String::valueOf).collect(Collectors.joining(","));

        var underlying = new IllegalStateException(("The %s bind of port(s) %s to %s would conflict with existing %s port bindings on %s.").formatted(
                proposedScope.name().toLowerCase(Locale.ROOT),
                portConflicts,
                proposedBindingInterface,
                existingBindingScope.name().toLowerCase(Locale.ROOT),

                existingBindingInterface));
        return buildOverviewException(virtualClusterName, seenVirtualClusters, underlying);
    }

    private IllegalStateException buildTlsConflictException(Integer port, String bindingAddress) {
        return new IllegalStateException(
                "The shared bind of port %d to %s has conflicting TLS settings with existing port on the same interface.".formatted(port, bindingAddress));
    }

    private IllegalStateException buildOverviewException(String virtualClusterName, Set<String> seenVirtualClusters, IllegalStateException underlying) {
        var seenVirtualClustersString = seenVirtualClusters.stream().sorted().map(s -> "'" + s + "'").collect(Collectors.joining(","));
        return new IllegalStateException("Configuration for virtual cluster '%s' conflicts with configuration for virtual cluster%s: %s.".formatted(virtualClusterName,
                seenVirtualClusters.size() > 1 ? "s" : "", seenVirtualClustersString), underlying);
    }

    private String getBindingAddressAsString(ClusterEndpointConfigProvider cep) {
        return cep.getBindAddress().orElse(ANY);
    }
}
