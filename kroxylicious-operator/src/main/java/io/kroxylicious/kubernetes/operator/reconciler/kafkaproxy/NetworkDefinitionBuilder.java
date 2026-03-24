/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.time.Duration;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.operator.ProxyConfigStateData;
import io.kroxylicious.proxy.config.NettySettings;
import io.kroxylicious.proxy.config.NetworkDefinition;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Builds a {@link NetworkDefinition} from the {@code spec.network} section of a
 * {@link KafkaProxy} custom resource, mapping Go-style duration strings from the CRD
 * into the proxy configuration model.
 */
final class NetworkDefinitionBuilder {

    private NetworkDefinitionBuilder() {
    }

    @Nullable
    static NetworkDefinition build(KafkaProxy proxy) {
        var spec = proxy.getSpec();
        if (spec == null) {
            return null;
        }
        var network = spec.getNetwork();
        if (network == null) {
            return null;
        }
        var mgmt = network.getManagement();
        var prxy = network.getProxy();
        return new NetworkDefinition(
                mgmt == null ? null
                        : buildNettySettings(mgmt.getWorkerThreadCount(), mgmt.getShutdownQuietPeriod(), null, null),
                prxy == null ? null
                        : buildNettySettings(prxy.getWorkerThreadCount(), prxy.getShutdownQuietPeriod(),
                                prxy.getAuthenticatedIdleTimeout(), prxy.getUnauthenticatedIdleTimeout()));
    }

    private static NettySettings buildNettySettings(@Nullable Integer workerThreadCount,
                                                    @Nullable String shutdownQuietPeriod,
                                                    @Nullable String authenticatedIdleTimeout,
                                                    @Nullable String unauthenticatedIdleTimeout) {
        return new NettySettings(
                Optional.ofNullable(workerThreadCount),
                Optional.ofNullable(shutdownQuietPeriod).map(NetworkDefinitionBuilder::parseDuration),
                Optional.empty(),
                Optional.ofNullable(authenticatedIdleTimeout).map(NetworkDefinitionBuilder::parseDuration),
                Optional.ofNullable(unauthenticatedIdleTimeout).map(NetworkDefinitionBuilder::parseDuration));
    }

    static Duration parseDuration(String value) {
        try {
            return ProxyConfigStateData.CONFIG_OBJECT_MAPPER.readValue('"' + value + '"', Duration.class);
        }
        catch (JsonProcessingException e) {
            // The CRD schema pattern validation should prevent invalid values reaching here
            throw new IllegalStateException("Invalid duration in KafkaProxy spec: '" + value + "'", e);
        }
    }
}
