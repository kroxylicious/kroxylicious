/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import io.kroxylicious.proxy.topology.VirtualNode;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Internal implementation of {@link VirtualNode}.
 *
 * <p>A null {@code virtualNodeId} means "any broker on the route" (produced by
 * {@link io.kroxylicious.proxy.router.RouterContext#anyNode}). A non-null
 * {@code virtualNodeId} identifies a specific target-cluster broker (produced by
 * {@link io.kroxylicious.proxy.router.RouterContext#nodeForId} or
 * {@link io.kroxylicious.proxy.router.RouterContext#virtualNode}).</p>
 */
record VirtualNodeImpl(String route, @Nullable Integer virtualNodeId) implements VirtualNode {}
