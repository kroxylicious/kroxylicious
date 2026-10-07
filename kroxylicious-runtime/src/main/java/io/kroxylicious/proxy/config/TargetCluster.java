/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.bootstrap.BootstrapSelectionStrategy;
import io.kroxylicious.proxy.bootstrap.RoundRobinBootstrapSelectionStrategy;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Represents the target (upstream) kafka cluster.
 *
 * @param bootstrapServers A list of host/port pairs to use for establishing the initial connection to the target (upstream) Kafka cluster.
 * @param tls tls configuration if a secure connection is to be used.
 * @param selectionStrategy The strategy used for selecting a bootstrap server when multiple servers are specified,
 *                          or null when the default (round-robin) strategy is to be used.
 * @param connectTimeout The maximum time to wait for a TCP connection to an upstream broker to be established,
 *                       or null when the default is to be used.
 */
public record TargetCluster(@JsonProperty(value = "bootstrapServers", required = true) String bootstrapServers,
                            @JsonProperty(value = "tls") Optional<Tls> tls,
                            @Nullable @JsonProperty(value = "bootstrapServerSelection") BootstrapSelectionStrategy selectionStrategy,
                            @Nullable @JsonProperty(value = "connectTimeout") Duration connectTimeout) {

    private static final BootstrapSelectionStrategy DEFAULT_SELECTION_STRATEGY = new RoundRobinBootstrapSelectionStrategy();
    // Matches Netty's io.netty.channel.DefaultChannelConfig.DEFAULT_CONNECT_TIMEOUT (30s), so leaving
    // connectTimeout unset preserves Netty's existing CONNECT_TIMEOUT_MILLIS default exactly.
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Validates the target cluster, stripping whitespace from {@code bootstrapServers}.
     */
    @JsonCreator
    public TargetCluster {
        if (bootstrapServers == null) {
            throw new IllegalArgumentException("'bootstrapServers' is required in a target cluster.");
        }
        bootstrapServers = bootstrapServers.replaceAll("\\s", "");
        // A null connectTimeout is allowed and means "use the default"; the resolved value is supplied
        // by effectiveConnectTimeout() at the use site so that round-trip serialization preserves null.
        // Zero is rejected because Netty interprets CONNECT_TIMEOUT_MILLIS == 0 as "no timeout at all",
        // which would silently disable the bound rather than set a short one.
        if (connectTimeout != null) {
            if (connectTimeout.isZero() || connectTimeout.isNegative()) {
                throw new IllegalArgumentException("'connectTimeout' for a target cluster must be positive, got: " + connectTimeout);
            }
            if (connectTimeout.toMillis() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "'connectTimeout' for a target cluster must not exceed " + Integer.MAX_VALUE + " ms, got: " + connectTimeout);
            }
        }
    }

    /**
     * Convenience constructor using the default (round-robin) bootstrap server selection strategy.
     *
     * @param bootstrapServers comma separated list of host/port pairs
     * @param tls tls configuration if a secure connection is to be used
     */
    public TargetCluster(String bootstrapServers, @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<Tls> tls) {
        this(bootstrapServers, tls, DEFAULT_SELECTION_STRATEGY);
    }

    /**
     * Convenience constructor using the default connect timeout.
     *
     * @param bootstrapServers comma separated list of host/port pairs
     * @param tls tls configuration if a secure connection is to be used
     * @param selectionStrategy the bootstrap server selection strategy, or null for the default
     */
    public TargetCluster(String bootstrapServers,
                         @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<Tls> tls,
                         @Nullable BootstrapSelectionStrategy selectionStrategy) {
        this(bootstrapServers, tls, selectionStrategy, null);
    }

    /**
     * A list of host/port pairs to use for establishing the initial connection to the target (upstream) Kafka cluster.
     * This list should be in the form host1:port1,host2:port2,...
     *
     * @return comma separated list of bootstrap servers.
     */
    @Override
    public String bootstrapServers() {
        return bootstrapServers;
    }

    /**
     * The configured bootstrap servers, parsed into host/port pairs.
     *
     * @return list of bootstrap server addresses
     */
    public List<HostPort> bootstrapServersList() {
        return Arrays.stream(bootstrapServers.split(",")).map(HostPort::parse).toList();
    }

    /**
     * The bootstrap server selection strategy in effect: the configured {@link #selectionStrategy()},
     * or the default (round-robin) strategy when none is configured.
     * <p>
     * The strategy is immutable configuration; the selection state itself is obtained per upstream
     * cluster model via {@link BootstrapSelectionStrategy#newSelector()}.
     *
     * @return the effective selection strategy, never null
     */
    // the default is not applied to the field itself so that we can maintain fidelity between the fluent API and the yaml config.
    public BootstrapSelectionStrategy effectiveSelectionStrategy() {
        return Objects.requireNonNullElse(selectionStrategy, DEFAULT_SELECTION_STRATEGY);
    }

    /**
     * The upstream connect timeout in effect: the configured {@link #connectTimeout()}, or the default
     * (30 seconds, matching Netty's own default) when none is configured. Applied as
     * {@link io.netty.channel.ChannelOption#CONNECT_TIMEOUT_MILLIS} on the upstream bootstrap.
     *
     * @return the effective connect timeout, never null
     */
    // the default is not applied to the field itself so that we can maintain fidelity between the fluent API and the yaml config.
    public Duration effectiveConnectTimeout() {
        return Objects.requireNonNullElse(connectTimeout, DEFAULT_CONNECT_TIMEOUT);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TargetCluster[");
        sb.append("bootstrapServers='").append(bootstrapServers).append('\'');
        sb.append(", tls=").append(tls.map(Tls::toString).orElse(null));
        sb.append(", bootstrapServerSelectionStrategy=").append(effectiveSelectionStrategy().getClass().getSimpleName());
        sb.append(", connectTimeout=").append(effectiveConnectTimeout());
        sb.append(']');
        return sb.toString();
    }
}
