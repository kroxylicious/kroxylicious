/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Namespace utils.
 */
public class NamespaceUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceUtils.class);
    private static final String TRACKED_NAMESPACES_KEY = "tracked.namespaces";

    private NamespaceUtils() {
    }

    /**
     * Delete namespace with wait.
     *
     * @param namespace the namespace
     */
    public static void deleteNamespaceWithWait(String namespace) {
        deleteNamespace(namespace, true);
    }

    /**
     * Delete namespace without wait.
     *
     * @param namespace the namespace
     */
    public static void deleteNamespaceWithoutWait(String namespace) {
        deleteNamespace(namespace, false);
    }

    private static void deleteNamespace(String namespace, boolean wait) {
        if (!Environment.SKIP_TEARDOWN) {
            LOGGER.info("Deleting namespace: {}", namespace);
            if (namespace.equals(Environment.STRIMZI_NAMESPACE) && Environment.SKIP_STRIMZI_INSTALL) {
                LOGGER.info("Skipped namespace: {}, because SKIP_STRIMZI_INSTALL is true", namespace);
                return;
            }
            kubeClient().deleteNamespace(namespace);
            if (wait) {
                await().atMost(Constants.GLOBAL_TIMEOUT).pollInterval(Constants.GLOBAL_POLL_INTERVAL)
                        .until(() -> kubeClient().getNamespace(namespace) == null);
            }

            LOGGER.info("Namespace: {} deleted", namespace);
        }
        else {
            LOGGER.info("Skipped deletion of {} as {} was true", namespace, Environment.SKIP_TEARDOWN_ENV);
        }
    }

    /**
     * Create namespace with wait.
     *
     * @param namespace the namespace
     */
    private static void createNamespaceWithWait(String namespace) {
        LOGGER.info("Creating namespace: {}", namespace);
        if (DeploymentUtils.isNamespaceCreated(namespace)) {
            LOGGER.warn("{} Namespace was already created!", namespace);
            return;
        }
        kubeClient().createNamespace(namespace);
        await().atMost(Constants.GLOBAL_TIMEOUT).pollInterval(Constants.GLOBAL_POLL_INTERVAL)
                .until(() -> DeploymentUtils.isNamespaceCreated(namespace));

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
    private static ExtensionContext.Store getStore(String storeName) {
        return ResourceManager.getTestContext().getStore(ExtensionContext.Namespace.create(storeName));
    }

    /**
     * Add namespace to set.
     *
     * @param namespaceName the namespace name
     * @param testSuiteName the test suite name
     */
    public static void addNamespaceToSet(String namespaceName, String testSuiteName) {
        Set<String> namespacesList = getOrCreateNamespacesForTestClass(testSuiteName);
        namespacesList.add(namespaceName);
    }

    /**
     * Gets namespaces for test class, creating the namespace if none exists.
     *
     * @param testClass the test class
     * @return the namespaces for test class
     */
    @SuppressWarnings("unchecked")
    public static Set<String> getOrCreateNamespacesForTestClass(String testClass) {
        return getStore(testClass).getOrComputeIfAbsent(TRACKED_NAMESPACES_KEY, s -> ConcurrentHashMap.newKeySet(), Set.class);
    }

    private static void deleteNamespaceFromSet(String namespaceName, String testSuiteName) {
        getOrCreateNamespacesForTestClass(testSuiteName).remove(namespaceName);
    }

    /**
     * For all entries inside the store it deletes all Namespaces in the particular Set
     * After that, it clears the whole Map
     * It is used mainly in {@code AbstractST.afterAllMayOverride} to remove everything after all test cases are executed
     */
    public static void deleteAllNamespacesFromSet(boolean async) {
        final String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();
        Set<String> namespaceList = getOrCreateNamespacesForTestClass(testSuiteName);
        LOGGER.info("Deleting all namespaces for {}", testSuiteName);

        namespaceList.forEach(ns -> {
            List<CompletableFuture<Void>> waiters = new ArrayList<>();
            CompletableFuture<Void> cf = CompletableFuture.runAsync(() -> deleteNamespaceWithWait(ns));
            if (async) {
                waiters.add(cf);
            }
            else {
                cf.join();
            }

            if (!waiters.isEmpty()) {
                CompletableFuture.allOf(waiters.toArray(new CompletableFuture[0])).join();
            }
        });
        getStore(testSuiteName).remove(TRACKED_NAMESPACES_KEY, Set.class);
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
     * Deletes Namespace with {@param namespaceName}, and in case that {@param collectorElement}
     * is not {@code null}, removes the Namespace from the store.
     *
     * @param namespaceName Name of the Namespace that should be deleted
     * @param testSuiteName the test suite name
     */
    public static void deleteNamespaceWithoutWaitAndRemoveFromSet(String namespaceName, String testSuiteName) {
        deleteNamespaceWithoutWait(namespaceName);
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
