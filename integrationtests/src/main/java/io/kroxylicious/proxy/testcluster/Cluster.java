/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testcluster;

import java.util.Map;

public interface Cluster extends AutoCloseable {
    /**
     * starts the cluster.  use #close will stop it again.
     */
    void start();

    /**
     * Gets the bootstrap servers for this cluster
     * @return bootstrap servers
     */
    String getBootstrapServers();

    /**
     * Gets the kafka connect config for this cluster.  Details such the bootstrap and SASL configuration
     * are provided automatically.  The return map may be muted by the caller.
     *
     * @return mutable kafka connect config map
     */
    Map<String, Object> getConnectConfigForCluster();
}
