/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kubernetes;

import io.fabric8.kubernetes.api.model.ServiceAccount;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.ResourceType;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

public class ServiceAccountResource implements ResourceType<ServiceAccount> {

    @Override
    public String getKind() {
        return Constants.SERVICE_ACCOUNT;
    }

    @Override
    public ServiceAccount get(String namespace, String name) {
        return kubeClient(namespace).getServiceAccount(namespace, name);
    }

    @Override
    public void create(ServiceAccount resource) {
        kubeClient().createOrUpdateServiceAccount(resource);
    }

    @Override
    public void delete(ServiceAccount resource) {
        kubeClient().deleteServiceAccount(resource);
    }

    @Override
    public void update(ServiceAccount resource) {
        kubeClient().createOrUpdateServiceAccount(resource);
    }

    @Override
    public boolean waitForReadiness(ServiceAccount resource) {
        return resource != null;
    }
}
