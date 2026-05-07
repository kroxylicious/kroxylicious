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
 * @param targetCluster the cluster being proxied
 * @param gateways virtual cluster gateways
 * @param logNetwork if true, network will be logged
 * @param logFrames if true, kafka rpcs will be logged
 * @param filters filers.
 * @param subjectBuilder subject builder configuration (optional)
 * @param topicNameCache topic-name cache configuration (optional)
 * @param drainTimeout maximum time to wait for in-flight requests to complete during
 *                     graceful connection draining for this cluster. Must be strictly
 *                     less than the Netty shutdown timeout (configured via
 *                     {@code network.proxy.shutdownTimeout}, default 15 s) — otherwise
 *                     Netty's force-close runs first and the per-connection drain timer
 *                     never fires. {@code null} means "use the proxy's default" — the
 *                     resolved value is supplied at the use site by
 *                     {@link #effectiveDrainTimeout()}. The default is currently 10 s
 *                     and may evolve in future proxy versions.
 */
@SuppressWarnings("java:S1123") // suppressing the spurious warning about missing @deprecated in javadoc. It is the field that is deprecated, not the class.
public record VirtualCluster(@JsonProperty(required = true) String name,
                             @JsonProperty(required = true) TargetCluster targetCluster,
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
        Objects.requireNonNull(targetCluster);
        if (!isDnsLabel(name)) {
            throw new IllegalConfigurationException(
                    "Virtual cluster name '" + name + "' is invalid. It must be less than 64 characters long and match pattern " + DNS_LABEL_PATTERN.pattern()
                            + " (case insensitive)");
        }
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

    public VirtualCluster(@JsonProperty(required = true) String name,
                          @JsonProperty(required = true) TargetCluster targetCluster,
                          @JsonProperty(required = true) List<VirtualClusterGateway> gateways,
                          boolean logNetwork,
                          boolean logFrames,
                          @Nullable List<String> filters) {
        this(name, targetCluster, gateways, logNetwork, logFrames, filters, null, null, null);
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
     * runtime always sees a resolved {@link Duration}.
     */
    Duration effectiveDrainTimeout() {
        return drainTimeout == null ? DEFAULT_DRAIN_TIMEOUT : drainTimeout;
    }

}
