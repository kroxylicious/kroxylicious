/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Namespace utils.
 */
public class NamespaceUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceUtils.class);
    private static final String NAMESPACES_KEY = "namespaces";

    private NamespaceUtils() {
    }

    /**
     * Is namespace created boolean.
     *
     * @param namespace the namespace
     * @return the boolean
     */
    public static boolean isNamespaceCreated(String namespace) {
        return kubeClient().getNamespace(namespace) != null;
    }

    /**
     * Delete namespace with wait.
     *
     * @param namespace the namespace
     */
    public static void deleteNamespaceWithWait(String namespace) {
        LOGGER.info("Deleting namespace: {}", namespace);
        kubeClient().deleteNamespace(namespace);
        await().atMost(Constants.GLOBAL_TIMEOUT).pollInterval(Constants.GLOBAL_POLL_INTERVAL)
                .until(() -> kubeClient().getNamespace(namespace) == null);

        LOGGER.info("Namespace: {} deleted", namespace);
    }

    /**
     * Create namespace with wait.
     *
     * @param namespace the namespace
     */
    public static void createNamespaceWithWait(String namespace) {
        LOGGER.info("Creating namespace: {}", namespace);
        if (isNamespaceCreated(namespace)) {
            LOGGER.warn("{} Namespace was already created!", namespace);
            return;
        }
        kubeClient().createNamespace(namespace);
        await().atMost(Constants.GLOBAL_TIMEOUT).pollInterval(Constants.GLOBAL_POLL_INTERVAL)
                .until(() -> isNamespaceCreated(namespace));

        LOGGER.info("Namespace: {} created", namespace);
    }

    /**
     * Overloads {@link #createNamespaceAndPrepare(String, String)}
     *
     * @param namespaceName name of Namespace that should be created
     */
    public static void createNamespaceAndPrepare(String namespaceName) {
        final String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();
        createNamespaceAndPrepare(namespaceName, testSuiteName);
    }

    /**
     * Method does following:
     *  - creates Namespace and waits for its readiness
     *  - applies default NetworkPolicy settings
     *  - copies image pull secrets from `default` Namespace
     *
     * @param namespaceName name of the Namespace that should be created and prepared
     * @param testSuiteName the test suite name
     */
    public static void createNamespaceAndPrepare(String namespaceName, String testSuiteName) {
        createNamespaceAndAddToSet(namespaceName, testSuiteName);
        DeploymentUtils.registryCredentialsSecret(namespaceName);
    }

    /**
     * Gets store.
     *
     * @param storeName the store name
     * @return the store
     */
    public static ExtensionContext.Store getStore(String storeName) {
        return ResourceManager.getTestContext().getStore(ExtensionContext.Namespace.create(storeName));
    }

    /**
     * Add namespace to set.
     *
     * @param namespaceName the namespace name
     * @param testSuiteName the test suite name
     */
    public static synchronized void addNamespaceToSet(String namespaceName, String testSuiteName) {
        if (!isNamespaceStored(testSuiteName, namespaceName)) {
            Set<String> namespacesList = getNamespacesForTestClass(testSuiteName);
            namespacesList.add(namespaceName);
            getStore(testSuiteName).put(NAMESPACES_KEY, namespacesList);
        }
    }

    private static boolean isNamespaceStored(String storeName, String namespace) {
        Set<String> namespaces = getNamespacesForTestClass(storeName);
        return namespaces.contains(namespace);
    }

    /**
     * Gets namespaces for test class.
     *
     * @param testClass the test class
     * @return the namespaces for test class
     */
    public static Set<String> getNamespacesForTestClass(String testClass) {
        Set<String> namespaces = getStore(testClass).get(NAMESPACES_KEY, Set.class);
        return namespaces != null ? namespaces : new HashSet<>();
    }

    private static synchronized void deleteNamespaceFromSet(String namespaceName, String testSuiteName) {
        Set<String> namespaceList = getNamespacesForTestClass(testSuiteName);
        namespaceList.remove(namespaceName);
        getStore(testSuiteName).put(NAMESPACES_KEY, namespaceList);
    }

    /**
     * For all entries inside the store it deletes all Namespaces in the particular Set
     * After that, it clears the whole Map
     * It is used mainly in {@code AbstractST.afterAllMayOverride} to remove everything after all test cases are executed
     */
    public static void deleteAllNamespacesFromSet() {
        final String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();
        Set<String> namespaceList = getNamespacesForTestClass(testSuiteName);
        namespaceList.forEach(NamespaceUtils::deleteNamespaceWithWait);
        if(!namespaceList.isEmpty()) {
            getStore(testSuiteName).remove(NAMESPACES_KEY);
        }
    }

    /**
     * Deletes Namespace with {@param namespaceName}, waits for its deletion, and in case that {@param collectorElement}
     * is not {@code null}, removes the Namespace from the store.
     *
     * @param namespaceName Name of the Namespace that should be deleted
     * @param testSuiteName the test suite name
     */
    public static void deleteNamespaceWithWaitAndRemoveFromSet(String namespaceName, String testSuiteName) {
        deleteNamespaceWithWait(namespaceName);
        deleteNamespaceFromSet(namespaceName, testSuiteName);
    }

    /**
     * Method for creating Namespace with {@param namespaceName}, waiting for its creation, and adding it
     * to the store.
     * The last step is done only in case that {@param collectorElement} is not {@code null}
     *
     * @param namespaceName name of Namespace that should be created and added to the Set
     * @param testSuiteName the test suite name
     */
    public static void createNamespaceAndAddToSet(String namespaceName, String testSuiteName) {
        createNamespaceWithWait(namespaceName);
        addNamespaceToSet(namespaceName, testSuiteName);
    }
}
