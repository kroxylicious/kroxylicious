/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

/**
 * Sealed hierarchy representing how a virtual cluster reaches its upstream Kafka cluster(s).
 * <ul>
 *   <li>{@link DirectRouting} — a single, statically-configured upstream cluster</li>
 *   <li>{@link DynamicRouting} — one or more upstream clusters reached via a named router plugin</li>
 * </ul>
 */
public sealed interface RoutingModel extends AutoCloseable permits DirectRouting, DynamicRouting {
    @Override
    default void close() {
    }
}
