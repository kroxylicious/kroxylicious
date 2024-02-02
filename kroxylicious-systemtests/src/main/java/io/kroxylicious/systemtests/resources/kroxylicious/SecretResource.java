/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kroxylicious;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Secret resource.
 */
public class SecretResource implements ResourceType<Secret> {
    @Override
    public String getKind() {
        return Constants.SECRET_KIND;
    }

    @Override
    public Secret get(String namespace, String name) {
        return secretClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(Secret resource) {
        secretClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(Secret resource) {
        secretClient().inNamespace(resource.getMetadata().getNamespace()).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(Secret resource) {
        secretClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(Secret resource) {
        return resource != null;
    }

    /**
     * Secret client mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<Secret, SecretList, Resource<Secret>> secretClient() {
        return kubeClient().getClient().resources(Secret.class, SecretList.class);
    }
}
