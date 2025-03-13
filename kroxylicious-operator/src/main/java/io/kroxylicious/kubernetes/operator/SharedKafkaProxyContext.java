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

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.config.RuntimeDecl;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

/**
 * Encapsulates access to the mutable state in the {@link Context<KafkaProxy>} shared between
 * {@link ProxyReconciler} its dependent resources.
 */
public class SharedKafkaProxyContext {

    private SharedKafkaProxyContext() {
    }

    static final String RUNTIME_DECL_KEY = "runtime";
    static final String CLUSTER_CONDITIONS_KEY = "cluster_conditions";

    /**
     * Set the RuntimeDecl
     */
    static void runtimeDecl(Context<KafkaProxy> context, RuntimeDecl runtimeDecl) {
        context.managedWorkflowAndDependentResourceContext().put(RUNTIME_DECL_KEY, runtimeDecl);
    }

    /**
     * Get the RuntimeDecl
     */
    static RuntimeDecl runtimeDecl(Context<KafkaProxy> context) {
        return context.managedWorkflowAndDependentResourceContext().getMandatory(RUNTIME_DECL_KEY, RuntimeDecl.class);
    }

    /**
     * Associate a condition with a specific cluster.
     */
    static void addClusterCondition(Context<KafkaProxy> context, VirtualKafkaCluster cluster, ClusterCondition clusterCondition) {
        Map<String, ClusterCondition> map = context.managedWorkflowAndDependentResourceContext().get(CLUSTER_CONDITIONS_KEY, Map.class).orElse(null);
        if (map == null) {
            map = Collections.synchronizedMap(new HashMap<>());
            context.managedWorkflowAndDependentResourceContext().put(CLUSTER_CONDITIONS_KEY, map);
        }
        map.put(name(cluster), clusterCondition);
    }

    static boolean isBroken(Context<KafkaProxy> context, VirtualKafkaCluster cluster) {
        Map<String, ClusterCondition> map = context.managedWorkflowAndDependentResourceContext().get(CLUSTER_CONDITIONS_KEY, Map.class).orElse(Map.of());
        return map.containsKey(name(cluster));
    }

    /**
     * Get the conditions specific to a cluster
     * @param context The context
     * @param cluster The cluster
     * @return The conditions specific to the given cluster; or empty if there were none.
     */
    static ClusterCondition clusterCondition(Context<KafkaProxy> context, VirtualKafkaCluster cluster) {
        Optional<Map<String, ClusterCondition>> map = (Optional) context.managedWorkflowAndDependentResourceContext().get(CLUSTER_CONDITIONS_KEY, Map.class);
        return map.orElse(Map.of()).getOrDefault(name(cluster), ClusterCondition.accepted(name(cluster)));
    }
}
