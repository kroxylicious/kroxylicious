/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.topology;

/**
 * Opaque reference to a node in the virtual cluster topology.
 *
 * <p>Routers obtain {@code VirtualNode} instances from
 * {@link io.kroxylicious.proxy.router.RouterContext RouterContext} methods and pass them back to
 * {@link io.kroxylicious.proxy.router.RouterContext#sendRequest RouterContext.sendRequest}. Implementations provide
 * {@code equals}/{@code hashCode} so that {@code VirtualNode}
 * instances can be used as map keys.</p>
 *
 * <p>This type is intentionally opaque — routers should not
 * inspect or construct instances directly. The runtime provides
 * the implementation.</p>
 */
public interface VirtualNode {
}
