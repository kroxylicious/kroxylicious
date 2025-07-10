/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import io.kroxylicious.proxy.service.NodeIdentificationStrategy;

public interface NodeIdentificationStrategyFactory {

    NodeIdentificationStrategy buildStrategy(String clusterName);

}
