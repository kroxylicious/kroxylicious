/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyspec.Clusters;
import io.kroxylicious.kubernetes.operator.config.RuntimeDecl;

/**
 * Encapsulates access to the mutable state in the {@link Context<KafkaProxy>} shared between
 * {@link ProxyReconciler} its dependent resources.
 */
public class SharedKafkaProxyContext {

    private SharedKafkaProxyContext() {
    }

    private static final String RUNTIME_DECL_KEY = "runtime";
    private static final String ERROR_KEY = "error";

    /**
     * Set the RuntimeDecl
     */
    static void runtimeDecl(Context<KafkaProxy> context, RuntimeDecl runtimeDecl) {
        context.managedDependentResourceContext().put(RUNTIME_DECL_KEY, runtimeDecl);
    }

    /**
     * Get the RuntimeDecl
     */
    static RuntimeDecl runtimeDecl(Context<KafkaProxy> context) {
        return context.managedDependentResourceContext().getMandatory(RUNTIME_DECL_KEY, RuntimeDecl.class);
    }

    /**
     * Add some error specific to a cluster
     */
    static void addClusterError(Context<KafkaProxy> context, Clusters cluster, Exception error) {
        Map<String, List<Exception>> map = context.managedDependentResourceContext().get(ERROR_KEY, Map.class).orElse(null);
        if (map == null) {
            map = Collections.synchronizedMap(new HashMap<>());
            context.managedDependentResourceContext().put(ERROR_KEY, map);
        }
        var exceptions = map.computeIfAbsent(cluster.getName(), k -> Collections.synchronizedList(new ArrayList<>()));
        exceptions.add(error);
    }

    /**
     * Get the errors specific to a cluster
     * @param context The context
     * @param cluster The cluster
     * @return The errors specific to the given cluster; or empty if there were none.
     */
    static List<Exception> clusterErrors(Context<KafkaProxy> context, Clusters cluster) {
        Optional<Map<String, List<Exception>>> map = (Optional) context.managedDependentResourceContext().get(ERROR_KEY, Map.class);
        return map.orElse(Map.of()).getOrDefault(cluster.getName(), List.of());
    }

    static List<Exception> allErrors(Context<KafkaProxy> context) {
        Optional<Map<String, List<Exception>>> map = (Optional) context.managedDependentResourceContext().get(ERROR_KEY, Map.class);
        return map.orElse(Map.of())
                .values()
                .stream()
                .flatMap(Collection::stream)
                .toList();
    }

}
