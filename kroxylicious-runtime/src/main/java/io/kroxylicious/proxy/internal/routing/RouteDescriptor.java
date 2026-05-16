/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.List;
import java.util.Objects;

import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.TargetCluster;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Runtime representation of a resolved route within a router.
 *
 * @param name the route name
 * @param targetCluster the target cluster for this route, or null if targeting a nested router
 * @param routerName the name of a nested router to forward to, or null if targeting a cluster
 * @param filters per-route filter definitions (may be empty)
 */
public record RouteDescriptor(
                              String name,
                              @Nullable TargetCluster targetCluster,
                              @Nullable String routerName,
                              List<NamedFilterDefinition> filters) {

    public RouteDescriptor {
        Objects.requireNonNull(name);
        Objects.requireNonNull(filters);
        if ((targetCluster == null) == (routerName == null)) {
            throw new IllegalArgumentException(
                    "Route '" + name + "' must specify exactly one of targetCluster or routerName");
        }
    }

    /**
     * Returns true if this route targets a Kafka cluster.
     */
    public boolean targetsCluster() {
        return targetCluster != null;
    }

    /**
     * Returns true if this route targets a nested router.
     */
    public boolean targetsRouter() {
        return routerName != null;
    }
}
