/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.ConfigurationDefinition;
import io.kroxylicious.proxy.service.Context;

public class TestClusterNetworkAddressConfigProviderContributor implements ClusterNetworkAddressConfigProviderContributor {

    public static final String SHORT_NAME = "test";

    @NonNull
    @Override
    public String getTypeName() {
        return SHORT_NAME;
    }

    @NonNull
    @Override
    public ConfigurationDefinition getConfigDefinition() {
        return new ConfigurationDefinition(Config.class, true);
    }

    @NonNull
    @Override
    public ClusterNetworkAddressConfigProvider getInstance(Context context) {
        return new TestClusterNetworkAddressConfigProvider(SHORT_NAME, context.getConfig(), context);
    }
}
