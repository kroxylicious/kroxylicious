/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Objects;

import io.kroxylicious.proxy.clusternetworkaddressconfigprovider.ClusterNetworkAddressConfigProviderContributor;
import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.ConfigurationDefinition;
import io.kroxylicious.proxy.service.Context;

public class TestClusterNetworkAddressConfigProviderContributor implements ClusterNetworkAddressConfigProviderContributor {

    public static final String SHORT_NAME = "test";

    @Override
    public boolean contributes(String shortName) {
        return Objects.equals(shortName, SHORT_NAME);
    }

    @Override
    public Class<? extends BaseConfig> getConfigType(String shortName) {
        if (!Objects.equals(shortName, SHORT_NAME)) {
            return null;
        }
        return Config.class;
    }

    @Override
    public ConfigurationDefinition getConfigDefinition(String shortName) {
        if (Objects.equals(shortName, SHORT_NAME)) {
            return new ConfigurationDefinition(Config.class, true);
        }
        return null;
    }

    @Override
    public ClusterNetworkAddressConfigProvider getInstance(String shortName, Context context) {
        if (!Objects.equals(shortName, SHORT_NAME)) {
            return null;
        }
        return new TestClusterNetworkAddressConfigProvider(shortName, context.getConfig(), context);
    }
}