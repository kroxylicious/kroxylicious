/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.List;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Context supplied to each {@link RoutingGraphVisitor} callback during a routing graph walk.
 * <p>
 * Carries the edge that led to the current node (the route and its containing router),
 * whether this is a first or revisit, and the ordered path of router names traversed
 * from the walk entry point to the current node.
 *
 * @param currentRoute  the route whose target is the node being visited; {@code null} at
 *                      the virtual-cluster entry point (no route led to the node)
 * @param sourceRouter  the router that contains {@code currentRoute}; {@code null} when
 *                      {@code currentRoute} is {@code null}
 * @param isFirstVisit  {@code true} if this is the first time the walker visits this router
 *                      during the current walk; {@code false} signals a revisit — the router
 *                      is already on the current DFS path, forming a cycle
 * @param path          unmodifiable list of router names from the walk entry point to the
 *                      current node, inclusive; empty at the virtual-cluster entry point
 */
public record WalkContext(
                          @Nullable RouteDefinition currentRoute,
                          @Nullable RouterDefinition sourceRouter,
                          boolean isFirstVisit,
                          List<String> path) {}
