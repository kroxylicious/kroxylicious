/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.databind.util.StdConverter;

import io.kroxylicious.proxy.internal.clusterendpointprovider.ClusterEndpointConfigProviderContributorManager;
import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;

public class ClusterEndpointConfigProviderConverter extends StdConverter<ClusterEndpointConfigProviderDefinition, ClusterEndpointConfigProvider> {

    @Override
    public ClusterEndpointConfigProvider convert(ClusterEndpointConfigProviderDefinition value) {
        return ClusterEndpointConfigProviderContributorManager.getInstance()
                .getClusterEndpointConfigProvider(value.type(), value.config());

    }

}
