/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.internal.net.EndpointListener;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

/**
 * Detects potential for port conflicts arising between virtual cluster configurations.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class PortConflictDetector {

    private static final Optional<String> ANY_INTERFACE = Optional.empty();
    private static final String ANY_STRING = "<any>";

    private enum BindingScope {
        SHARED,
        EXCLUSIVE
    }

    /**
     * Validates the configuration throwing an exception if a conflict is detected.
     *
     * @param virtualClusterModelMap map of virtual clusters.
     * @param otherExclusivePort an optional exclusive port that should conflict with virtual cluster ports
     */
    public void validate(Collection<VirtualClusterModel> virtualClusterModelMap, Optional<HostPort> otherExclusivePort) {

        Set<String> seenVirtualClusters = new HashSet<>();

        Map<Optional<String>, Set<Integer>> inUseExclusivePorts = new HashMap<>();
        Map<Optional<String>, Map<Integer, Boolean>> inUseSharedPorts = new HashMap<>();

        virtualClusterModelMap.stream()
                .sorted(Comparator.comparing(VirtualClusterModel::getClusterName))
                .forEach(virtualCluster -> {
                    var name = virtualCluster.getClusterName();

                    virtualCluster.listeners()
                            .forEach((listenerName, listener) -> doValidate(otherExclusivePort, listener, name, inUseExclusivePorts, seenVirtualClusters, inUseSharedPorts));

                });
    }

    private void doValidate(Optional<HostPort> otherExclusivePort, EndpointListener listener, String name, Map<Optional<String>, Set<Integer>> inUseExclusivePorts,
                            Set<String> seenVirtualClusters, Map<Optional<String>, Map<Integer, Boolean>> inUseSharedPorts) {
        var proposedSharedPorts = listener.getSharedPorts();
        var proposedExclusivePorts = listener.getExclusivePorts();

        if (otherExclusivePort.isPresent()) {
            Optional<String> otherInterface = otherExclusivePort.map(hostPort -> hostPort.host().equals("0.0.0.0") ? null : hostPort.host());
            var checkPorts = listener.getBindAddress().isEmpty() || otherInterface.isEmpty() || listener.getBindAddress().equals(otherInterface);
            if (checkPorts) {
                checkForConflictsWithOtherExclusivePort(otherExclusivePort.get(), name, listener, proposedExclusivePorts, BindingScope.EXCLUSIVE);
                checkForConflictsWithOtherExclusivePort(otherExclusivePort.get(), name, listener, proposedSharedPorts, BindingScope.SHARED);
            }
        }

        // if this virtual cluster is binding to <any>, we need to check for conflicts on the <any> interface and *all* specific interfaces,
        // otherwise we just check for conflicts on <any> and the specified specific interface
        var exclusiveCheckSet = listener.getBindAddress().isEmpty() ? inUseExclusivePorts.keySet()
                : Set.of(ANY_INTERFACE, listener.getBindAddress());

        // check the proposed *exclusive ports* for conflicts with the *exclusive ports* seen so far.
        assertExclusivePortsAreMutuallyExclusive(seenVirtualClusters, inUseExclusivePorts, listener, name, proposedExclusivePorts, exclusiveCheckSet);

        // check the proposed *shared ports* for conflicts with the *exclusive ports* seen so far.
        assertSharedPortsDoNotOverlapWithExlusivePorts(seenVirtualClusters, inUseExclusivePorts, listener, name, proposedSharedPorts,
                exclusiveCheckSet);

        // check the proposed *exclusive ports* for conflicts with the *shared ports* seen so far.
        assertExclusivePortsDoNotOverlapWithExclusivePorts(seenVirtualClusters, inUseSharedPorts, listener, name, proposedExclusivePorts,
                exclusiveCheckSet);

        // check the proposed *shared ports* for conflicts with the shared ports seen so far on *different* interfaces.
        assertSharedPortsAreExclusiveAcrossInterfaces(seenVirtualClusters, inUseSharedPorts, listener, name, proposedSharedPorts, exclusiveCheckSet);

        // check for proposed shared ports for differing TLS configuration
        assertSharedPortsHaveMatchingTlsConfiguration(seenVirtualClusters, inUseSharedPorts, listener, name, proposedSharedPorts);

        seenVirtualClusters.add(name);
        inUseExclusivePorts.computeIfAbsent(listener.getBindAddress(), k -> new HashSet<>()).addAll(proposedExclusivePorts);
        var sharedMap = inUseSharedPorts.computeIfAbsent(listener.getBindAddress(), k -> new HashMap<>());
        proposedSharedPorts.forEach(p -> sharedMap.put(p, listener.isUseTls()));
    }

    private void assertSharedPortsHaveMatchingTlsConfiguration(Set<String> seenVirtualClusters, Map<Optional<String>, Map<Integer, Boolean>> inUseSharedPorts,
                                                               EndpointListener virtualClusterModel, String name,
                                                               Set<Integer> proposedSharedPorts) {
        proposedSharedPorts.forEach(p -> {
            var tls = inUseSharedPorts.getOrDefault(virtualClusterModel.getBindAddress(), Map.of()).get(p);
            if (tls != null && !tls.equals(virtualClusterModel.isUseTls())) {
                throw buildOverviewException(name, seenVirtualClusters, buildTlsConflictException(p, virtualClusterModel.getBindAddress()));
            }
        });
    }

    private void assertSharedPortsAreExclusiveAcrossInterfaces(Set<String> seenVirtualClusters, Map<Optional<String>, Map<Integer, Boolean>> inUseSharedPorts,
                                                               EndpointListener virtualClusterModel, String name,
                                                               Set<Integer> proposedSharedPorts, Set<Optional<String>> exclusiveCheckSet) {
        var sharedCheckSet = new HashSet<>(exclusiveCheckSet);
        sharedCheckSet.remove(virtualClusterModel.getBindAddress());

        inUseSharedPorts.entrySet().stream()
                .filter(interfacePortsEntry -> sharedCheckSet.contains(interfacePortsEntry.getKey()))
                .forEach(entry -> {
                    var bindingInterface = entry.getKey();
                    var ports = entry.getValue().keySet();
                    var conflicts = getSortedPortConflicts(ports, proposedSharedPorts);
                    if (!conflicts.isEmpty()) {
                        throw buildPortConflictException(name, seenVirtualClusters, conflicts,
                                virtualClusterModel.getBindAddress(), BindingScope.SHARED,
                                bindingInterface, BindingScope.SHARED);
                    }
                });
    }

    private void assertExclusivePortsDoNotOverlapWithExclusivePorts(Set<String> seenVirtualClusters, Map<Optional<String>, Map<Integer, Boolean>> inUseSharedPorts,
                                                                    EndpointListener virtualClusterModel, String name,
                                                                    Set<Integer> proposedExclusivePorts, Set<Optional<String>> exclusiveCheckSet) {
        inUseSharedPorts.entrySet().stream()
                .filter(interfacePortsEntry -> exclusiveCheckSet.contains(interfacePortsEntry.getKey()))
                .forEach(entry -> {
                    var bindingInterface = entry.getKey();
                    var ports = entry.getValue().keySet();
                    var conflicts = getSortedPortConflicts(ports, proposedExclusivePorts);
                    if (!conflicts.isEmpty()) {
                        throw buildPortConflictException(name, seenVirtualClusters, conflicts,
                                virtualClusterModel.getBindAddress(), BindingScope.EXCLUSIVE,
                                bindingInterface, BindingScope.SHARED);
                    }
                });
    }

    private void assertSharedPortsDoNotOverlapWithExlusivePorts(Set<String> seenVirtualClusters, Map<Optional<String>, Set<Integer>> inUseExclusivePorts,
                                                                EndpointListener virtualClusterModel, String name,
                                                                Set<Integer> proposedSharedPorts, Set<Optional<String>> exclusiveCheckSet) {
        inUseExclusivePorts.entrySet().stream()
                .filter(interfacePortsEntry -> exclusiveCheckSet.contains(interfacePortsEntry.getKey()))
                .forEach(entry -> {
                    var bindingInterface = entry.getKey();
                    var ports = entry.getValue();
                    var conflicts = getSortedPortConflicts(ports, proposedSharedPorts);
                    if (!conflicts.isEmpty()) {
                        throw buildPortConflictException(name, seenVirtualClusters, conflicts,
                                virtualClusterModel.getBindAddress(), BindingScope.SHARED,
                                bindingInterface, BindingScope.EXCLUSIVE);
                    }
                });
    }

    private void assertExclusivePortsAreMutuallyExclusive(Set<String> seenVirtualClusters, Map<Optional<String>, Set<Integer>> inUseExclusivePorts,
                                                          EndpointListener virtualClusterModel, String name,
                                                          Set<Integer> proposedExclusivePorts, Set<Optional<String>> exclusiveCheckSet) {
        inUseExclusivePorts.entrySet().stream()
                .filter(interfacePortsEntry -> exclusiveCheckSet.contains(interfacePortsEntry.getKey()))
                .forEach(entry -> {
                    var bindingInterface = entry.getKey();
                    var ports = entry.getValue();
                    var conflicts = getSortedPortConflicts(ports, proposedExclusivePorts);
                    if (!conflicts.isEmpty()) {
                        throw buildPortConflictException(name, seenVirtualClusters, conflicts,
                                virtualClusterModel.getBindAddress(), BindingScope.EXCLUSIVE,
                                bindingInterface, BindingScope.EXCLUSIVE);
                    }
                });
    }

    private void checkForConflictsWithOtherExclusivePort(HostPort otherHostPort, String name, EndpointListener cluster, Set<Integer> proposedExclusivePorts,
                                                         BindingScope scope) {
        var ports = Set.of(otherHostPort.port());
        var conflicts = getSortedPortConflicts(ports, proposedExclusivePorts);
        if (!conflicts.isEmpty()) {
            Optional<String> proposedBindingInterface = cluster.getBindAddress();
            var portConflicts = conflicts.stream().map(String::valueOf).collect(Collectors.joining(","));

            throw new IllegalStateException("The %s bind of port(s) %s for virtual cluster '%s' to %s would conflict with another (non-cluster) port binding".formatted(
                    scope.name().toLowerCase(Locale.ROOT),
                    portConflicts,
                    name,
                    proposedBindingInterface.orElse(ANY_STRING)));
        }
    }

    private List<Integer> getSortedPortConflicts(Set<Integer> ports, Set<Integer> candidates) {
        return ports.stream().filter(candidates::contains).sorted().toList();
    }

    private IllegalStateException buildPortConflictException(String virtualClusterName, Set<String> seenVirtualClusters, List<Integer> conflicts,
                                                             Optional<String> proposedBindingInterface, BindingScope proposedScope,
                                                             Optional<String> existingBindingInterface, BindingScope existingBindingScope) {
        var portConflicts = conflicts.stream().map(String::valueOf).collect(Collectors.joining(","));

        var underlying = new IllegalStateException("The %s bind of port(s) %s to %s would conflict with existing %s port bindings on %s.".formatted(
                proposedScope.name().toLowerCase(Locale.ROOT),
                portConflicts,
                proposedBindingInterface.orElse(ANY_STRING),
                existingBindingScope.name().toLowerCase(Locale.ROOT),
                existingBindingInterface.orElse(ANY_STRING)));
        return buildOverviewException(virtualClusterName, seenVirtualClusters, underlying);
    }

    private IllegalStateException buildTlsConflictException(Integer port, Optional<String> bindingAddress) {
        return new IllegalStateException(
                "The shared bind of port %d to %s has conflicting TLS settings with existing port on the same interface.".formatted(port,
                        bindingAddress.orElse(ANY_STRING)));
    }

    private IllegalStateException buildOverviewException(String virtualClusterName, Set<String> seenVirtualClusters, IllegalStateException underlying) {
        var seenVirtualClustersString = seenVirtualClusters.stream().sorted().map(s -> "'" + s + "'").collect(Collectors.joining(","));
        return new IllegalStateException("Configuration for virtual cluster '%s' conflicts with configuration for virtual cluster%s: %s.".formatted(virtualClusterName,
                seenVirtualClusters.size() > 1 ? "s" : "", seenVirtualClustersString), underlying);
    }

}
