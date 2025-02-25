/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.operator.config.RuntimeDecl;
import io.kroxylicious.kubernetes.proxy.api.v1alpha1.Proxy;
import io.kroxylicious.kubernetes.proxy.api.v1alpha1.proxyspec.Clusters;

/**
 * Encapsulates access to the mutable state in the {@link Context<Proxy>} shared between
 * {@link ProxyReconciler} its dependent resources.
 */
public class SharedProxyContext {

    private SharedProxyContext() {
    }

    static final String RUNTIME_DECL_KEY = "runtime";
    static final String CLUSTER_CONDITIONS_KEY = "cluster_conditions";

    /**
     * Set the RuntimeDecl
     */
    static void runtimeDecl(Context<Proxy> context, RuntimeDecl runtimeDecl) {
        context.managedDependentResourceContext().put(RUNTIME_DECL_KEY, runtimeDecl);
    }

    /**
     * Get the RuntimeDecl
     */
    static RuntimeDecl runtimeDecl(Context<Proxy> context) {
        return context.managedDependentResourceContext().getMandatory(RUNTIME_DECL_KEY, RuntimeDecl.class);
    }

    /**
     * Associate a condition with a specific cluster.
     */
    static void addClusterCondition(Context<Proxy> context, Clusters cluster, ClusterCondition clusterCondition) {
        Map<String, ClusterCondition> map = context.managedDependentResourceContext().get(CLUSTER_CONDITIONS_KEY, Map.class).orElse(null);
        if (map == null) {
            map = Collections.synchronizedMap(new HashMap<>());
            context.managedDependentResourceContext().put(CLUSTER_CONDITIONS_KEY, map);
        }
        map.put(cluster.getName(), clusterCondition);
    }

    static boolean isBroken(Context<Proxy> context, Clusters cluster) {
        Map<String, ClusterCondition> map = context.managedDependentResourceContext().get(CLUSTER_CONDITIONS_KEY, Map.class).orElse(Map.of());
        return map.containsKey(cluster.getName());
    }

    /**
     * Get the conditions specific to a cluster
     * @param context The context
     * @param cluster The cluster
     * @return The conditions specific to the given cluster; or empty if there were none.
     */
    static ClusterCondition clusterCondition(Context<Proxy> context, Clusters cluster) {
        Optional<Map<String, ClusterCondition>> map = (Optional) context.managedDependentResourceContext().get(CLUSTER_CONDITIONS_KEY, Map.class);
        return map.orElse(Map.of()).getOrDefault(cluster.getName(), ClusterCondition.accepted(cluster.getName()));
    }

}
