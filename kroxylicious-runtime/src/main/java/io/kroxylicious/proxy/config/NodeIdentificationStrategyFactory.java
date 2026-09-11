/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import io.kroxylicious.proxy.service.NodeIdentificationStrategy;

/**
 * A factory for {@link NodeIdentificationStrategy} instances, implemented by the configuration
 * objects (port based and SNI based) that describe how a gateway identifies the target node
 * of an incoming connection.
 */
public interface NodeIdentificationStrategyFactory {

    /**
     * Builds the node identification strategy for the given virtual cluster.
     *
     * @param clusterName name of the virtual cluster
     * @return the node identification strategy
     */
    NodeIdentificationStrategy buildStrategy(String clusterName);

}
