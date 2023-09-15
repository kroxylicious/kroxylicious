/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.k8s.exception;

import java.util.List;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;

public class KubeClient {

    protected final KubernetesClient client;
    protected String namespace;

    public KubeClient(KubernetesClient client, String namespace) {
        this.client = client;
        this.namespace = namespace;
    }

    // ============================
    // ---------> CLIENT <---------
    // ============================

    public KubernetesClient getClient() {
        return client;
    }

    // ===============================
    // ---------> NAMESPACE <---------
    // ===============================

    public KubeClient namespace(String futureNamespace) {
        return new KubeClient(this.client, futureNamespace);
    }

    public String getNamespace() {
        return namespace;
    }

    public Namespace getNamespace(String namespace) {
        return client.namespaces().withName(namespace).get();
    }

    public void createNamespace(String namespaceName) {
        Namespace ns = new NamespaceBuilder().withNewMetadata().withName(namespaceName).endMetadata().build();
        client.namespaces().resource(ns).create();
    }

    public void deleteNamespace(String name) {
        client.namespaces().withName(name).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    // =========================
    // ---------> POD <---------
    // =========================

    public List<Pod> listPods(String namespaceName, LabelSelector selector) {
        return client.pods().inNamespace(namespaceName).withLabelSelector(selector).list().getItems();
    }

    public List<Pod> listPods(String namespaceName) {
        return client.pods().inNamespace(namespaceName).list().getItems();
    }

    /**
     * Returns list of pods by prefix in pod name
     * @param namespaceName Namespace name
     * @param podNamePrefix prefix with which the name should begin
     * @return List of pods
     */
    public List<Pod> listPodsByPrefixInName(String namespaceName, String podNamePrefix) {
        return listPods(namespaceName)
                .stream().filter(p -> p.getMetadata().getName().startsWith(podNamePrefix))
                .collect(Collectors.toList());
    }

    /**
     * Gets pod
     */
    public Pod getPod(String namespaceName, String name) {
        return client.pods().inNamespace(namespaceName).withName(name).get();
    }

    public Pod getPod(String name) {
        return getPod(getNamespace(), name);
    }

    // ================================
    // ---------> DEPLOYMENT <---------
    // ================================

    public Deployment createOrReplaceDeployment(Deployment deployment) {
        return client.apps().deployments().inNamespace(deployment.getMetadata().getNamespace()).resource(deployment).create();
    }

    /**
     * Gets deployment
     */
    public Deployment getDeployment(String namespaceName, String deploymentName) {
        return client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).get();
    }

    /**
     * Gets deployment status
     */
    public boolean getDeploymentStatus(String namespaceName, String deploymentName) {
        return client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).isReady();
    }

    public String logsInSpecificNamespace(String namespaceName, String podName) {
        return client.pods().inNamespace(namespaceName).withName(podName).getLog();
    }
}
