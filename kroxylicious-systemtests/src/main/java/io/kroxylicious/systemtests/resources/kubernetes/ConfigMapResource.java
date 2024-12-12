/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kubernetes;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type config map resource.
 */
public class ConfigMapResource implements ResourceType<ConfigMap> {
    @Override
    public String getKind() {
        return Constants.CONFIG_MAP;
    }

    @Override
    public ConfigMap get(String namespace, String name) {
        return configClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(ConfigMap resource) {
        configClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(ConfigMap resource) {
        configClient().inNamespace(resource.getMetadata().getNamespace()).withName(
                resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(ConfigMap resource) {
        configClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(ConfigMap resource) {
        return true;
    }

    /**
     * Config client mixed operation.
     *
     * @return the mixed operation
     */
    public static MixedOperation<ConfigMap, ConfigMapList, Resource<ConfigMap>> configClient() {
        return kubeClient().getClient().resources(ConfigMap.class, ConfigMapList.class);
    }
}
