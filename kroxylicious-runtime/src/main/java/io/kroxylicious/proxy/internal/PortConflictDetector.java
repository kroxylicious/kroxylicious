/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Stream;

import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

/**
 * Detects potential for port conflicts arising between virtual cluster configurations.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class PortConflictDetector {

    private static final String ANY_STRING = "<any>";

    private enum BindingScope {
        SHARED,
        EXCLUSIVE
    }

    private record BindAddress(Optional<String> address) {
        boolean overlaps(BindAddress other) {
            return isAnyHost() || other.isAnyHost() || address.equals(other.address);
        }

        boolean isAnyHost() {
            return address.isEmpty();
        }

        @Override
        public String toString() {
            return address.orElse(ANY_STRING);
        }
    }

    private record CandidateBinding(EndpointGateway gateway, int port, BindAddress bindAddress, BindingScope scope) {
        void validateNonConflicting(CandidateBinding other) {
            if (this.port == other.port && bindAddress.overlaps(other.bindAddress)) {
                if (scope == BindingScope.EXCLUSIVE || other.scope == BindingScope.EXCLUSIVE) {
                    throw conflictException(other, "exclusive port collision");
                }
                if (!bindAddress.equals(other.bindAddress)) {
                    throw conflictException(other, "shared port cannot bind to different hosts");
                }
                if (gateway.isUseTls() != other.gateway.isUseTls()) {
                    throw conflictException(other, "shared port cannot be both TLS and non-TLS");
                }
            }
        }

        private IllegalStateException conflictException(CandidateBinding other, String reason) {
            return new IllegalStateException(this + " conflicts with " + other + ": " + reason);
        }

        void validateNonConflicting(HostPort otherExclusivePort) {
            boolean isAnyHost = otherExclusivePort.host().equals("0.0.0.0");
            BindAddress otherBindAddress = new BindAddress(isAnyHost ? Optional.empty() : Optional.of(otherExclusivePort.host()));
            if (this.port == otherExclusivePort.port() && bindAddress.overlaps(otherBindAddress)) {
                throw new IllegalStateException(this + " conflicts with another (non-cluster) exclusive port " + otherExclusivePort);
            }
        }

        @Override
        public String toString() {
            return scope.name().toLowerCase(Locale.ENGLISH) + " " + (gateway.isUseTls() ? "TLS" : "TCP") + " bind of " + bindAddress + ":" + port +
                    " for gateway '" + gateway.name() + "' of virtual cluster '" + gateway.virtualCluster().getClusterName() + "'";
        }
    }

    /**
     * Validates the configuration throwing an exception if a conflict is detected.
     *
     * @param virtualClusterModelMap map of virtual clusters.
     * @param otherExclusivePort an optional exclusive port that should conflict with virtual cluster ports
     */
    public void validate(Collection<VirtualClusterModel> virtualClusterModelMap, Optional<HostPort> otherExclusivePort) {
        List<CandidateBinding> candidateBindings = candidateBindings(virtualClusterModelMap);
        validateCandidatesDoNotConflictWithOtherExclusivePort(candidateBindings, otherExclusivePort);
        validateSharedPortsRequireServerNameIndication(candidateBindings);
        validateCandidatesDoNotConflictWithEachOther(candidateBindings);
    }

    private void validateSharedPortsRequireServerNameIndication(List<CandidateBinding> candidateBindings) {
        candidateBindings.stream().filter(binding -> binding.scope == BindingScope.SHARED).forEach(binding -> {
            if (!binding.gateway.requiresServerNameIndication()) {
                throw new IllegalStateException(
                        binding + " is misconfigured, shared port bindings must use server name indication, or connections cannot be routed correctly");
            }
        });
    }

    private static void validateCandidatesDoNotConflictWithOtherExclusivePort(List<CandidateBinding> candidateBindings, Optional<HostPort> otherExclusivePort) {
        otherExclusivePort.ifPresent(hostPort -> candidateBindings.forEach(c -> c.validateNonConflicting(hostPort)));
    }

    private static void validateCandidatesDoNotConflictWithEachOther(List<CandidateBinding> candidateBindings) {
        // compare all unique combinations
        for (int i = 0; i < candidateBindings.size(); i++) {
            for (int j = i + 1; j < candidateBindings.size(); j++) {
                candidateBindings.get(i).validateNonConflicting(candidateBindings.get(j));
            }
        }
    }

    private static List<CandidateBinding> candidateBindings(Collection<VirtualClusterModel> virtualClusterModelMap) {
        return virtualClusterModelMap.stream().sorted(
                Comparator.comparing(VirtualClusterModel::getClusterName))
                .flatMap(m -> m.gateways().values().stream().sorted(Comparator.comparing(EndpointGateway::name))).flatMap(gateway -> {
                    Stream<CandidateBinding> exclusiveBindings = gateway.getExclusivePorts().stream().sorted()
                            .map(exclusivePort -> new CandidateBinding(gateway, exclusivePort, new BindAddress(gateway.getBindAddress()), BindingScope.EXCLUSIVE));
                    Stream<CandidateBinding> sharedBindings = gateway.getSharedPorts().stream().sorted()
                            .map(sharedPort -> new CandidateBinding(gateway, sharedPort, new BindAddress(gateway.getBindAddress()), BindingScope.SHARED));
                    return Stream.concat(exclusiveBindings, sharedBindings);
                }).toList();
    }

}
