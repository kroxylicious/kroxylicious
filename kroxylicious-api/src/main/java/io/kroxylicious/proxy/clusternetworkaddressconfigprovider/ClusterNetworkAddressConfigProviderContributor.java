/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.clusternetworkaddressconfigprovider;

import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.Contributor;

/**
 * ClusterNetworkAddressConfigProviderContributor is a pluggable source of network address information.
 * @see Contributor
 */
public interface ClusterNetworkAddressConfigProviderContributor extends Contributor<ClusterNetworkAddressConfigProvider> {
}
