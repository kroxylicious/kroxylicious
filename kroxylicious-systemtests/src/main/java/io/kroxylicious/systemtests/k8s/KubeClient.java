/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s;

import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonDeletingOperation;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

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
     * List pods.
     *
     * @param namespaceName the namespace name
     * @return the list
     */
    public List<Pod> listPods(String namespaceName) {
        return client.pods().inNamespace(namespaceName).list().getItems();
    }

    /**
     * List pods.
     *
     * @param namespaceName the namespace name
     * @param selector the selector
     * @return the list
     */
    public List<Pod> listPods(String namespaceName, LabelSelector selector) {
        return client.pods().inNamespace(namespaceName).withLabelSelector(selector).list().getItems();
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
                .toList();
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
     * Create or update deployment deployment.
     *
     * @param deployment the deployment
     */
    public void createOrUpdateDeployment(Deployment deployment) {
        client.apps().deployments().inNamespace(deployment.getMetadata().getNamespace()).resource(deployment).createOr(NonDeletingOperation::update);
    }

    /**
     * Delete deployment.
     *
     * @param namespaceName the namespace name
     * @param deploymentName the deployment name
     */
    public void deleteDeployment(String namespaceName, String deploymentName) {
        client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    /**
     * Update deployment.
     *
     * @param deployment the deployment
     */
    public void updateDeployment(Deployment deployment) {
        client.apps().deployments().inNamespace(deployment.getMetadata().getNamespace()).resource(deployment).update();
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
     * Gets deployment selectors
     * @param namespaceName the namespace name
     * @param deploymentName the deployment name
     * @return the deployment selectors
     */
    public LabelSelector getPodSelectorFromDeployment(String namespaceName, String deploymentName) {
        return client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).get().getSpec().getSelector();
    }

    /**
     * Gets if the deployment is ready
     * @param namespaceName the namespace name
     * @param deploymentName the deployment name
     * @return true if the deployment is ready, false otherwise
     */
    public boolean isDeploymentReady(String namespaceName, String deploymentName) {
        return client.apps().deployments().inNamespace(namespaceName).withName(deploymentName).isReady();
    }

    /**
     * Is deployment running.
     *
     * @param namespaceName the namespace name
     * @param podName the pod name
     * @return true if the deployment is running, false otherwise
     */
    public boolean isDeploymentRunning(String namespaceName, String podName) {
        return Optional.ofNullable(client.pods().inNamespace(namespaceName).withName(podName).get().getStatus()).map(PodStatus::getPhase)
                .map(s -> s.equalsIgnoreCase("running")).orElse(false);
    }

    /**
     * Is the pod run succeeded.
     *
     * @param namespaceName the namespace name
     * @param podName the pod name
     * @return true if the job is succeeded. false otherwise
     */
    public boolean isPodRunSucceeded(String namespaceName, String podName) {
        return Optional.ofNullable(client.pods().inNamespace(namespaceName).withName(podName).get().getStatus()).map(PodStatus::getPhase)
                .map(s -> s.equalsIgnoreCase("succeeded")).orElse(false);
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
     * Returns list of pods by prefix in pod name
     * @param namespaceName Namespace name
     * @param serviceNamePrefix the service name prefix
     * @return List of pods
     */
    public List<Service> listServicesByPrefixInName(String namespaceName, String serviceNamePrefix) {
        return client.services().inNamespace(namespaceName).list().getItems()
                .stream().filter(p -> p.getMetadata().getName().startsWith(serviceNamePrefix))
                .toList();
    }

    /**
     * Gets service name by prefix.
     *
     * @param namespaceName the namespace name
     * @param serviceNamePrefix the service name prefix
     * @return the service name
     */
    public String getServiceNameByPrefix(String namespaceName, String serviceNamePrefix) {
        DeploymentUtils.waitForServiceReady(namespaceName, serviceNamePrefix, Constants.GLOBAL_STATUS_TIMEOUT);
        List<Service> services = listServicesByPrefixInName(namespaceName, serviceNamePrefix);
        if (!services.isEmpty()) {
            return services.get(0).getMetadata().getName();
        }
        else {
            throw new KubeClusterException.NotFound("Service with prefix " + serviceNamePrefix + " not found!");
        }
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

    // =====================================
    // ---> CUSTOM RESOURCE DEFINITIONS <---
    // =====================================

    /**
     * Method for creating the specified CustomResourceDefinition.
     * In case that the CRD is already created, it is being updated.
     * This can be caused by not cleared CRDs from other tests or in case we shut down the test before the cleanup
     * phase.
     * The skip of the cleanup phase can then break the CO installation - because the resource already exists.
     * Without the update, we would need to manually remove all existing resources before running the test again.
     * It should not have an impact on the functionality, we just update the CRD.
     * @param resourceDefinition CustomResourceDefinition that we want to create or update
     */
    public void createOrUpdateCustomResourceDefinition(CustomResourceDefinition resourceDefinition) {
        client.apiextensions().v1().customResourceDefinitions().resource(resourceDefinition).createOr(NonDeletingOperation::update);
    }

    /**
     * Delete custom resource definition.
     *
     * @param resourceDefinition the resource definition
     */
    public void deleteCustomResourceDefinition(CustomResourceDefinition resourceDefinition) {
        client.apiextensions().v1().customResourceDefinitions().resource(resourceDefinition).delete();
    }

    /**
     * Gets custom resource definition.
     *
     * @param name the name
     * @return the custom resource definition
     */
    public CustomResourceDefinition getCustomResourceDefinition(String name) {
        return client.apiextensions().v1().customResourceDefinitions().withName(name).get();
    }
}
