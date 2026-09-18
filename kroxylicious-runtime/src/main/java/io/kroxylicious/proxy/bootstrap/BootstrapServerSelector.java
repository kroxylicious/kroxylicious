/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;

import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.ThreadSafe;

/**
 * Runtime counterpart of a {@link BootstrapSelectionStrategy}: selects the upstream bootstrap server
 * that a new connection should be made to, from the list of configured candidates.
 * <p>
 * A selector owns whatever mutable state its strategy needs (for example a round-robin position),
 * keeping that state out of the immutable configuration model. A selector is created per
 * {@link io.kroxylicious.proxy.internal.routing.UpstreamClusterModel} via
 * {@link BootstrapSelectionStrategy#newSelector()} and is then shared by every client connection to
 * that upstream cluster, so it is invoked concurrently from multiple Netty event-loop threads.
 * Implementations <strong>must</strong> therefore be thread-safe.
 */
@ThreadSafe
@FunctionalInterface
public interface BootstrapServerSelector {

    /**
     * Selects a bootstrap server from the given candidates.
     *
     * @param bootstrapServers the candidate bootstrap servers; must not be empty
     * @return the selected bootstrap server
     */
    HostPort select(List<HostPort> bootstrapServers);
}
