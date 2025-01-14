/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Service to build a {@link ClusterNetworkAddressConfigProvider} for a configuration. Implementations
 * should be annotated with {@link io.kroxylicious.proxy.plugin.Plugin} and added to the ServiceLoader
 * metadata for this interface.
 * @param <T> config type
 */
public interface ClusterNetworkAddressConfigProviderService<T> {

    /**
     * Build a cluster network address config provider
     * @param config config
     * @return cluster network address config provider
     */
    @NonNull
    ClusterNetworkAddressConfigProvider build(T config);

}
