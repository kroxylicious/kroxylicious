/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import java.util.Optional;

/**
 * A snapshot of coordinator assignments, returned by
 * {@link TopologyService#coordinators}.
 *
 * <p>This is a self-contained result — callers should use it
 * directly rather than querying the topology cache separately.
 * The key type is implicit (it was specified in the
 * {@code coordinators()} call that produced this result).</p>
 */
public interface Coordinators {

    /**
     * Returns the coordinator for the given key, or empty if the
     * coordinator was not discovered (e.g. the group or transaction
     * does not exist, or the coordinator is unavailable).
     *
     * @param key the group or transaction ID
     * @return the coordinator's virtual node, or empty
     */
    Optional<VirtualNode> coordinatorFor(String key);
}
