/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A virtual cluster.
 *
 * @param targetCluster the cluster being proxied
 * @param clusterNetworkAddressConfigProvider virtual cluster network config - deprecated - use a named listener
 * @param tls deprecated - tls settings for the virtual cluster - deprecated - use a named listener
 * @param listeners listeners
 * @param logNetwork if true, network will be logged
 * @param logFrames if true, kafka rpcs will be logged
 * @param filters filers.
 */
@SuppressWarnings("java:S1123") // suppressing the spurious warning about missing @deprecated in javadoc. It is the field that is deprecated, not the class.
public record VirtualCluster(TargetCluster targetCluster,
                             @Deprecated(forRemoval = true, since = "0.11.0") ClusterNetworkAddressConfigProviderDefinition clusterNetworkAddressConfigProvider,
                             @Deprecated(forRemoval = true, since = "0.11.0") @JsonProperty() Optional<Tls> tls,

                             @JsonProperty(required = false) Map<String, VirtualClusterListener> listeners,
                             boolean logNetwork,
                             boolean logFrames,
                             @Nullable List<String> filters) {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualCluster.class);

    @SuppressWarnings({ "removal", "java:S2789" }) // S2789 - checking for null tls is the intent
    public VirtualCluster {
        if (clusterNetworkAddressConfigProvider != null || (tls != null && tls.isPresent())) {
            if (clusterNetworkAddressConfigProvider == null) {
                throw new IllegalConfigurationException("Deprecated virtualCluster property 'tls' supplied, but 'clusterNetworkAddressConfigProvider' is null");
            }
            if (listeners == null || listeners.isEmpty()) {
                LOGGER.warn(
                        "The virtualCluster properties 'clusterNetworkAddressConfigProvider' and 'tls' are deprecated, specify virtual cluster listeners using the listeners map.");
                listeners = Map.of("default", new VirtualClusterListener(clusterNetworkAddressConfigProvider, tls));
            }
            else {
                throw new IllegalConfigurationException(
                        "When using listeners, the virtualCluster properties 'clusterNetworkAddressConfigProvider' and 'tls' must be omitted");
            }
        }
        if (listeners == null || listeners.isEmpty()) {
            throw new IllegalConfigurationException("no listeners configured for virtualCluster");
        }
        Set<String> keysWithNullValues = listeners.entrySet().stream().filter(e -> e.getValue() == null).map(Map.Entry::getKey).collect(Collectors.toSet());
        if (!keysWithNullValues.isEmpty()) {
            throw new IllegalConfigurationException("some listeners had null values: '" + keysWithNullValues + "'");
        }
    }

}
