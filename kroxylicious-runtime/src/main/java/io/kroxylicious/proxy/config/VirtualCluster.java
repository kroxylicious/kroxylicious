/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A virtual cluster.
 *
 * @param name virtual cluster name
 * @param targetCluster deprecated inline target cluster (mutually exclusive with {@code target})
 * @param target reference to a named cluster or router (mutually exclusive with {@code targetCluster})
 * @param gateways virtual cluster gateways
 * @param logNetwork if true, network will be logged
 * @param logFrames if true, kafka rpcs will be logged
 * @param filters filters applied to requests; when used with a router, these run before router dispatch
 * @param subjectBuilder subject builder configuration (optional)
 * @param topicNameCache topic-name cache configuration (optional)
 * @param drainTimeout maximum time to wait for in-flight requests to complete during
 *                     graceful connection draining for this cluster
 */
@SuppressWarnings("java:S1123")
public record VirtualCluster(@JsonProperty(required = true) String name,
                             @Deprecated @Nullable TargetCluster targetCluster,
                             @Nullable RouteDefinition.Target target,
                             @JsonProperty(required = true) List<VirtualClusterGateway> gateways,
                             boolean logNetwork,
                             boolean logFrames,
                             @Nullable List<String> filters,
                             @Nullable TransportSubjectBuilderConfig subjectBuilder,
                             @Nullable CacheConfiguration topicNameCache,
                             @Nullable Duration drainTimeout) {

    private static final Pattern DNS_LABEL_PATTERN = Pattern.compile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", Pattern.CASE_INSENSITIVE);
    private static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(10);

    @SuppressWarnings("java:S2789")
    public VirtualCluster {
        Objects.requireNonNull(name);
        if (!isDnsLabel(name)) {
            throw new IllegalConfigurationException(
                    "Virtual cluster name '" + name + "' is invalid. It must be less than 64 characters long and match pattern " + DNS_LABEL_PATTERN.pattern()
                            + " (case insensitive)");
        }
        validateTargetExclusivity(name, targetCluster, target);
        if (gateways == null || gateways.isEmpty()) {
            throw new IllegalConfigurationException("no gateways configured for virtual cluster '" + name + "'");
        }
        if (gateways.stream().anyMatch(Objects::isNull)) {
            throw new IllegalConfigurationException("one or more gateways were null for virtual cluster '" + name + "'");
        }
        validateNoDuplicatedGatewayNames(gateways);
        if (drainTimeout != null && (drainTimeout.isZero() || drainTimeout.isNegative())) {
            throw new IllegalConfigurationException(
                    "drainTimeout for virtual cluster '" + name + "' must be positive, got: " + drainTimeout);
        }
    }

    public VirtualCluster(String name,
                          TargetCluster targetCluster,
                          List<VirtualClusterGateway> gateways,
                          boolean logNetwork,
                          boolean logFrames,
                          @Nullable List<String> filters) {
        this(name, targetCluster, null, gateways, logNetwork, logFrames, filters, null, null, null);
    }

    /**
     * @return the name of the target router, or {@code null}
     */
    @Nullable
    public String router() {
        return target != null ? target.router() : null;
    }

    /**
     * @return the name of the target cluster (from {@code target}), or {@code null}
     */
    @Nullable
    public String namedTargetCluster() {
        return target != null ? target.cluster() : null;
    }

    private static void validateTargetExclusivity(String name,
                                                  @Nullable TargetCluster targetCluster,
                                                  @Nullable RouteDefinition.Target target) {
        if (targetCluster != null && target != null) {
            throw new IllegalConfigurationException(
                    "Virtual cluster '" + name + "' must specify exactly one of 'targetCluster' or 'target'");
        }
        if (targetCluster == null && target == null) {
            throw new IllegalConfigurationException(
                    "Virtual cluster '" + name + "' must specify exactly one of 'targetCluster' or 'target'");
        }
    }

    boolean isDnsLabel(String name) {
        if (name.length() > 63) {
            return false;
        }
        else {
            return DNS_LABEL_PATTERN.matcher(name).matches();
        }
    }

    private void validateNoDuplicatedGatewayNames(List<VirtualClusterGateway> gateways) {
        var names = gateways.stream()
                .map(VirtualClusterGateway::name)
                .toList();
        var duplicates = names.stream()
                .filter(i -> Collections.frequency(names, i) > 1)
                .collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalConfigurationException(
                    "Gateway names for a virtual cluster must be unique. The following gateway names are duplicated: [%s]".formatted(
                            String.join(", ", duplicates)));
        }
    }

    CacheConfiguration topicNameCacheConfig() {
        return topicNameCache == null ? CacheConfiguration.DEFAULT : topicNameCache;
    }

    Duration effectiveDrainTimeout() {
        return drainTimeout == null ? DEFAULT_DRAIN_TIMEOUT : drainTimeout;
    }

    public boolean sameAs(@Nullable VirtualCluster other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        return this.canonical().equals(other.canonical());
    }

    private VirtualCluster canonical() {
        List<VirtualClusterGateway> sortedGateways = gateways.stream()
                .sorted(java.util.Comparator.comparing(VirtualClusterGateway::name))
                .toList();
        return new VirtualCluster(
                name,
                targetCluster,
                target,
                sortedGateways,
                logNetwork,
                logFrames,
                filters,
                subjectBuilder,
                topicNameCache,
                drainTimeout);
    }

}
