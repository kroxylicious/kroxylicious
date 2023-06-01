/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.util.StdConverter;

import io.kroxylicious.proxy.internal.clusterendpointprovider.ClusterEndpointConfigProviderContributorManager;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;

public class ClusterEndpointConfigProviderConverter extends StdConverter<ClusterEndpointConfigProviderDefinition, ClusterNetworkAddressConfigProvider> {

    public ClusterEndpointConfigProviderConverter() {
        super();
    }

    @Override
    public JavaType getInputType(TypeFactory typeFactory) {
        return super.getInputType(typeFactory);
    }

    @Override
    public JavaType getOutputType(TypeFactory typeFactory) {
        return super.getOutputType(typeFactory);
    }

    @Override
    protected JavaType _findConverterType(TypeFactory tf) {
        return super._findConverterType(tf);
    }

    @Override
    public ClusterNetworkAddressConfigProvider convert(ClusterEndpointConfigProviderDefinition value) {
        return ClusterEndpointConfigProviderContributorManager.getInstance()
                .getClusterEndpointConfigProvider(value.type(), value.config());

    }

}
