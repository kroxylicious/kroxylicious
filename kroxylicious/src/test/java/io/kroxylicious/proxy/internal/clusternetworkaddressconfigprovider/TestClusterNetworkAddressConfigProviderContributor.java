/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import org.jetbrains.annotations.NotNull;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.Context;

import edu.umd.cs.findbugs.annotations.NonNull;

public class TestClusterNetworkAddressConfigProviderContributor implements ClusterNetworkAddressConfigProviderContributor<Void> {

    public static final String SHORT_NAME = "test";

    @NonNull
    @Override
    public String getTypeName() {
        return SHORT_NAME;
    }

    @NotNull
    @Override
    public Class<Void> getConfigType() {
        return Void.class;
    }

    @NonNull
    @Override
    public ClusterNetworkAddressConfigProvider getInstance(Context<Void> context) {
        return new TestClusterNetworkAddressConfigProvider(SHORT_NAME, context.getConfig(), context);
    }
}
