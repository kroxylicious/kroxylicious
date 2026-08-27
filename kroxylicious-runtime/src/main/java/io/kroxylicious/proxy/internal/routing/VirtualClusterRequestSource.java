/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import io.kroxylicious.proxy.internal.ClientConnectionStateMachine;

/**
 * Indicates that the enclosing {@link RoutingHandler} sits at the top of the routing
 * pipeline, receiving requests directly from the client. Carries the
 * {@link ClientConnectionStateMachine} that owns the client connection lifecycle.
 */
record VirtualClusterRequestSource(ClientConnectionStateMachine ccsm) implements RequestSource {}
