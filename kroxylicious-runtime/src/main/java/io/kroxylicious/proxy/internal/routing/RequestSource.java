/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

/**
 * Describes where requests reaching a {@link RoutingHandler} originate from.
 *
 * <p>A {@link VirtualClusterRequestSource} means the handler sits at the top of the
 * routing pipeline, receiving requests directly from the client. A
 * {@link RouterRequestSource} means it is nested — intercepting requests that a
 * parent router dispatched to a specific route.
 */
sealed interface RequestSource permits VirtualClusterRequestSource, RouterRequestSource {
}
