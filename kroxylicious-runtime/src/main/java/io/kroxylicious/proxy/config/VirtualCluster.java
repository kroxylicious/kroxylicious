/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
 * @param listeners virtual cluster listeners
 * @param logNetwork if true, network will be logged
 * @param logFrames if true, kafka rpcs will be logged
 * @param filters filers.
 */
@SuppressWarnings("java:S1123") // suppressing the spurious warning about missing @deprecated in javadoc. It is the field that is deprecated, not the class.
public record VirtualCluster(TargetCluster targetCluster,
                             @Deprecated(forRemoval = true, since = "0.11.0") ClusterNetworkAddressConfigProviderDefinition clusterNetworkAddressConfigProvider,
                             @Deprecated(forRemoval = true, since = "0.11.0") @JsonProperty() Optional<Tls> tls,

                             @JsonProperty(required = false) List<VirtualClusterListener> listeners,
                             boolean logNetwork,
                             boolean logFrames,
                             @Nullable List<String> filters) {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualCluster.class);

    /**
     * Name given to the listener defined using the deprecated fields.
     */
    @Deprecated(forRemoval = true, since = "0.11.0")
    static final String DEFAULT_LISTENER_NAME = "default";

    @SuppressWarnings({ "removal", "java:S2789" }) // S2789 - checking for null tls is the intent
    public VirtualCluster {
        if (clusterNetworkAddressConfigProvider != null || (tls != null && tls.isPresent())) {
            if (clusterNetworkAddressConfigProvider == null) {
                throw new IllegalConfigurationException("Deprecated virtualCluster property 'tls' supplied, but 'clusterNetworkAddressConfigProvider' is null");
            }
            if (listeners == null || listeners.isEmpty()) {
                LOGGER.warn(
                        "The virtualCluster properties 'clusterNetworkAddressConfigProvider' and 'tls' are deprecated, specify virtual cluster listeners using the listeners map.");
                listeners = List.of(new VirtualClusterListener(DEFAULT_LISTENER_NAME, clusterNetworkAddressConfigProvider, tls));
            }
            else {
                throw new IllegalConfigurationException(
                        "When using listeners, the virtualCluster properties 'clusterNetworkAddressConfigProvider' and 'tls' must be omitted");
            }
        }
        if (listeners == null || listeners.isEmpty()) {
            throw new IllegalConfigurationException("no listeners configured for virtualCluster");
        }
        if (listeners.stream().anyMatch(Objects::isNull)) {
            throw new IllegalConfigurationException("one or more listeners were null");
        }
        var names = listeners.stream()
                .map(VirtualClusterListener::name)
                .toList();
        var duplicates = names.stream()
                .filter(i -> Collections.frequency(names, i) > 1)
                .collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalConfigurationException("Listener names for a virtual cluster must be unique. The following listener names are duplicated: [%s]".formatted(String.join(", ", duplicates)));
        }
    }
}
