/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.clusterendpointprovider;

import io.kroxylicious.proxy.service.ClusterEndpointConfigProvider;
import io.kroxylicious.proxy.service.Contributor;

/**
 * ClusterEndpointConfigProvider is a pluggable source of Kroxylicious endpoint provider implementations.
 * @see Contributor
 */
public interface ClusterEndpointProviderContributor extends Contributor<ClusterEndpointConfigProvider> {
}
