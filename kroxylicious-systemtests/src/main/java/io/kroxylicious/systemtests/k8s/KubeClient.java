/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s;

import java.util.List;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * The type Kube client.
 */
public class KubeClient {

    /**
     * The Client.
     */
    protected final KubernetesClient client;
    /**
     * The Namespace.
     */
    protected String namespace;

    /**
     * Instantiates a new Kube client.
     *
     * @param client the client
     * @param namespace the namespace
     */
    public KubeClient(KubernetesClient client, String namespace) {
        this.client = client;
        this.namespace = namespace;
    }

    // ============================
    // ---------> CLIENT <---------
    // ============================

    /**
     * Gets client.
     *
     * @return the client
     */
    public KubernetesClient getClient() {
        return client;
    }

    // ===============================
    // ---------> NAMESPACE <---------
    // ===============================

    /**
     * Namespace kube client.
     *
     * @param futureNamespace the future namespace
     * @return the kube client
     */
    public KubeClient namespace(String futureNamespace) {
        return new KubeClient(this.client, futureNamespace);
    }

    /**
     * Gets namespace.
     *
     * @return the namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Gets namespace.
     *
     * @param namespace the namespace
     * @return the namespace
     */
    public Namespace getNamespace(String namespace) {
        return client.namespaces().withName(namespace).get();
    }

    /**
     * Create namespace.
     *
     * @param namespaceName the namespace name
     */
    public void createNamespace(String namespaceName) {
        Namespace ns = new NamespaceBuilder().withNewMetadata().withName(namespaceName).endMetadata().build();
        client.namespaces().resource(ns).create();
    }

    /**
     * Delete namespace.
     *
     * @param name the name
     */
    public void deleteNamespace(String name) {
        client.namespaces().withName(name).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    // =========================
    // ---------> POD <---------
    // =========================
    /**
     * List pods list.
     *
     * @param namespaceName the namespace name
     * @param labelKey the label key
     * @param labelValue the label value
     * @return the list
     */
    public List<Pod> listPods(String namespaceName, String labelKey, String labelValue) {
        return client.pods().inNamespace(namespaceName).withLabel(labelKey, labelValue).list().getItems();
    }

    /**
     * List pods list.
     *
     * @param namespaceName the namespace name
     * @return the list
     */
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
     * @param namespaceName the namespace name
     * @param name the name
     * @return the pod
     */
    public Pod getPod(String namespaceName, String name) {
        return client.pods().inNamespace(namespaceName).withName(name).get();
    }

    /**
     * Gets pod.
     *
     * @param name the name
     * @return the pod
     */
    public Pod getPod(String name) {
        return getPod(getNamespace(), name);
    }

    // ================================
    // ---------> DEPLOYMENT <---------
    // ================================

    /**
     * Create or replace deployment deployment.
     *
     * @param deployment the deployment
     * @return the deployment
     */
    public Deployment createOrReplaceDeployment(Deployment deployment) {
        return client.apps().deployments().inNamespace(deployment.getMetadata().getNamespace()).resource(deployment).create();
    }

    /**
     * Gets deployment
     * @param namespaceName the namespace name
     * @param deploymentName the deployment name
     * @return the deployment
     */
    public Deployment getDeployment(String namespaceName, String deploymentName) {
        return client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).get();
    }

    /**
     * Gets deployment status
     * @param namespaceName the namespace name
     * @param deploymentName the deployment name
     * @return the deployment status
     */
    public boolean isDeploymentReady(String namespaceName, String deploymentName) {
        return client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).isReady();
    }

    /**
     * Gets service.
     *
     * @param namespaceName the namespace name
     * @param deploymentName the deployment name
     * @return the service
     */
    public Service getService(String namespaceName, String deploymentName) {
        return client.services().inNamespace(namespaceName).withName(deploymentName).get();
    }

    /**
     * Logs in specific namespace string.
     *
     * @param namespaceName the namespace name
     * @param podName the pod name
     * @return the string
     */
    public String logsInSpecificNamespace(String namespaceName, String podName) {
        return client.pods().inNamespace(namespaceName).withName(podName).getLog();
    }
}
