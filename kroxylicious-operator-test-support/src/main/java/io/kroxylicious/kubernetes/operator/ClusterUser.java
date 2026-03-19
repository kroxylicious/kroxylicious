/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Represents a cluster user interacting with Kubernetes resources in a specific namespace.
 * <p>
 * This is distinct from the operator's own client: a {@code ClusterUser} holds a
 * Kubernetes client scoped to the RBAC of a regular user, not the operator itself.
 * Using this explicitly in tests makes clear which operations represent user actions
 * and which represent operator reactions, which matters for reasoning about RBAC.
 */
public class ClusterUser {

    private final KubernetesClient client;
    private final String namespace;

    ClusterUser(@NonNull KubernetesClient client, @NonNull String namespace) {
        this.client = client;
        this.namespace = namespace;
    }

    @NonNull
    public <T extends HasMetadata> T create(@NonNull T resource) {
        return client.resource(resource).inNamespace(namespace).create();
    }

    @Nullable
    public <T extends HasMetadata> T get(@NonNull Class<T> type, @NonNull String name) {
        return client.resources(type).inNamespace(namespace).withName(name).get();
    }

    @NonNull
    public <T extends HasMetadata> T replace(@NonNull T resource) {
        return client.resource(resource).inNamespace(namespace).update();
    }

    public <T extends HasMetadata> boolean delete(@NonNull T resource) {
        var result = client.resource(resource).inNamespace(namespace).delete();
        return result.size() == 1 && result.get(0).getCauses().isEmpty();
    }

    @NonNull
    public <T extends HasMetadata> NonNamespaceOperation<T, KubernetesResourceList<T>, Resource<T>> resources(@NonNull Class<T> type) {
        return client.resources(type).inNamespace(namespace);
    }
}
