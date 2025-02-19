/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;

/**
 * A virtual cluster listener.
 *
 * @param clusterNetworkAddressConfigProvider network config
 * @param tls tls settings
 */
public record VirtualClusterListener(@JsonProperty(required = true) ClusterNetworkAddressConfigProviderDefinition clusterNetworkAddressConfigProvider,
                                     Optional<Tls> tls) {

}
