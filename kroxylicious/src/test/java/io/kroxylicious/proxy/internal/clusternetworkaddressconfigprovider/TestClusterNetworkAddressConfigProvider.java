/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;

import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.Context;
import io.kroxylicious.proxy.service.HostPort;

public record TestClusterNetworkAddressConfigProvider(String shortName, Object config, Context context) implements ClusterNetworkAddressConfigProvider {
    @Override
    public HostPort getClusterBootstrapAddress() {
        throw new NotImplementedException("not implemented!");
    }

    @Override
    public HostPort getBrokerAddress(int nodeId) throws IllegalArgumentException {
        throw new NotImplementedException("not implemented!");
    }
}
