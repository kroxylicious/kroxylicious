/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import io.kroxylicious.proxy.config.VirtualCluster;

public interface VirtualClusterResolver {
    VirtualCluster resolve(String sniHostname, int targetPort);

    VirtualCluster resolve(int targetPort);
}
