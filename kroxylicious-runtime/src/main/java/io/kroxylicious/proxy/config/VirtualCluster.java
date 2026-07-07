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
 * @param targetCluster inline target cluster definition (mutually exclusive with {@code target})
 * @param target reference to a named cluster or router (mutually exclusive with {@code targetCluster})
 * @param gateways virtual cluster gateways
 * @param logNetwork if true, network will be logged
 * @param logFrames if true, kafka rpcs will be logged
 * @param filters filters applied to requests
 * @param subjectBuilder subject builder configuration (optional)
 * @param topicNameCache topic-name cache configuration (optional)
 * @param drainTimeout maximum time to wait for in-flight requests to complete during
 *                     graceful connection draining for this cluster
 */
@SuppressWarnings("java:S1123") // suppressing the spurious warning about missing @deprecated in javadoc. It is the field that is deprecated, not the class.
public record VirtualCluster(@JsonProperty(required = true) String name,
                             @Deprecated(since = "0.22.0", forRemoval = true) @Nullable TargetCluster targetCluster,
                             @Nullable RouteTarget target,
                             @JsonProperty(required = true) List<VirtualClusterGateway> gateways,
                             boolean logNetwork,
                             boolean logFrames,
                             @Nullable List<String> filters,
                             @Nullable TransportSubjectBuilderConfig subjectBuilder,
                             @Nullable CacheConfiguration topicNameCache,
                             @Nullable Duration drainTimeout) {

    private static final Pattern DNS_LABEL_PATTERN = Pattern.compile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", Pattern.CASE_INSENSITIVE);
    private static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(10);

    @SuppressWarnings("java:S2789") // S2789 - checking for null tls is the intent
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
        // Validate explicitly-supplied drainTimeout. A null drainTimeout is allowed and
        // means "use the proxy's default"; the resolved value is supplied by
        // effectiveDrainTimeout() at the use site so that round-trip serialization
        // preserves null and the operator-generated configs don't bake in today's default.
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

    @Nullable
    public String router() {
        return target != null ? target.router() : null;
    }

    @Nullable
    public String namedTargetCluster() {
        return target != null ? target.cluster() : null;
    }

    private static void validateTargetExclusivity(String name,
                                                  @Nullable TargetCluster targetCluster,
                                                  @Nullable RouteTarget target) {
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

    /**
     * Resolves the {@code drainTimeout} for this virtual cluster, applying the proxy's
     * default when the field is {@code null}. Used at the construction site of
     * {@link io.kroxylicious.proxy.model.VirtualClusterModel} so that the raw record
     * component remains nullable (preserving Jackson round-trip fidelity), while the
     * runtime always sees a resolved {@link java.time.Duration}.
     */
    Duration effectiveDrainTimeout() {
        return drainTimeout == null ? DEFAULT_DRAIN_TIMEOUT : drainTimeout;
    }

    /**
     * Returns {@code true} if this cluster's deployment-time configuration is semantically
     * identical to {@code other}'s. Used by the configuration change-detection pipeline
     * (see {@code VirtualClusterChangeDetector}) to decide whether a virtual cluster needs
     * to be restarted across a {@code reconfigure()}.
     *
     * <p>Implementation uses the record's auto-generated {@link #equals(Object)} after
     * canonicalising gateway order &mdash; {@code gateways} is a name-keyed semantic set,
     * so reordering YAML entries is a no-op and must not produce a false positive. All
     * other components (including any added in the future) are compared by the record's
     * auto-equals, so this method extends automatically.
     */
    public boolean sameAs(@Nullable VirtualCluster other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        return this.canonical().equals(other.canonical());
    }

    /**
     * Returns an equivalent {@link VirtualCluster} with gateways sorted by name. Used only
     * as an internal helper of {@link #sameAs(VirtualCluster)} to normalise the one
     * order-insensitive component before the record's auto-equals does the rest.
     */
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
