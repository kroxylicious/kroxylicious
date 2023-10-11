/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.clusternetworkaddressconfigprovider;

import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.Context;
import io.kroxylicious.proxy.service.Contributor;

/**
 * ClusterNetworkAddressConfigProviderContributor is a pluggable source of network address information.
 * @param <B> config type
 * @see Contributor
 * @deprecated this class is going to be made internal to kroxylicious until we decide to document/offer it as a plugin
 */
@Deprecated
public interface ClusterNetworkAddressConfigProviderContributor<B> extends Contributor<ClusterNetworkAddressConfigProvider, B, Context<B>> {
}
