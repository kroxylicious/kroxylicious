/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.service.HostPort;

/**
 * Represents the target (upstream) kafka cluster.
 */
public record TargetCluster(
                            @JsonProperty("bootstrapServers") String bootstrapServers,
                            @JsonProperty(value = "tls") Optional<Tls> tls) {

    private static final Logger LOGGER = LoggerFactory.getLogger(TargetCluster.class);

    @JsonCreator
    public static TargetCluster foo(
                                    @JsonProperty("bootstrapServers") String bootstrapServers,
                                    @Deprecated(since = "0.10.0", forRemoval = true) @JsonProperty("bootstrap_servers") String deprecatedBootstrapServers,
                                    @JsonProperty(value = "tls") Optional<Tls> tls) {
        if (bootstrapServers == null && deprecatedBootstrapServers == null) {
            throw new IllegalArgumentException("'bootstrapServers' is required in a target cluster.");
        }
        if (bootstrapServers != null && deprecatedBootstrapServers != null) {
            throw new IllegalArgumentException("'bootstrapServers' and 'bootstrap_servers' cannot both be specified in a target cluster.");
        }
        if (deprecatedBootstrapServers != null) {
            LOGGER.warn("'bootstrap_servers' in a target cluster is deprecated and will be removed in a future release: It should be changed to 'bootstrapServers'.");
            return new TargetCluster(deprecatedBootstrapServers, tls);
        }
        else {
            return new TargetCluster(bootstrapServers, tls);
        }
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

    public List<HostPort> bootstrapServersList() {
        return Arrays.stream(bootstrapServers.split(",")).map(HostPort::parse).toList();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TargetCluster[");
        sb.append("bootstrapServers='").append(bootstrapServers).append('\'');
        sb.append(", clientTls=").append(tls.map(Tls::toString).orElse(null));
        sb.append(']');
        return sb.toString();
    }
}
