/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kubernetes;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.Updatable;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class NamespaceResource implements ResourceType<Namespace> {
    @Override
    public String getKind() {
        return Constants.NAMESPACE;
    }

    @Override
    public Namespace get(String namespace, String name) {
        return namespaceClient().withName(name).get();
    }

    @Override
    public void create(Namespace resource) {
        namespaceClient().resource(resource).createOr(Updatable::update);
    }

    @Override
    public void delete(Namespace resource) {
        namespaceClient().withName(resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(Namespace resource) {
        namespaceClient().resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(Namespace resource) {
        return resource != null;
    }

    /**
     * service client mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<Namespace, NamespaceList, Resource<Namespace>> namespaceClient() {
        return kubeClient().getClient().resources(Namespace.class, NamespaceList.class);
    }
}
