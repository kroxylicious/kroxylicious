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
import io.kroxylicious.systemtests.logs.CollectorElement;
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
     * Overloads {@link #createNamespaceAndPrepare(String, CollectorElement)}, but it creates a new {@link CollectorElement} for
     * test class and test method automatically. The {@link CollectorElement} is needed for {@link io.kroxylicious.systemtests.logs.TestLogCollector} to
     * correctly collect logs from all the Namespaces -> even those that are created manually in the test cases.
     *
     * @param namespaceName name of Namespace that should be created
     */
    public static void createNamespaceAndPrepare(String namespaceName) {
        final String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();
        final String testCaseName = ResourceManager.getTestContext().getTestMethod().orElse(null) == null ? ""
                : ResourceManager.getTestContext().getRequiredTestMethod().getName();

        createNamespaceAndPrepare(namespaceName, new CollectorElement(testSuiteName, testCaseName));
    }

    /**
     * Method does following:
     *  - creates Namespace and waits for its readiness
     *  - applies default NetworkPolicy settings
     *  - copies image pull secrets from `default` Namespace
     *
     * @param namespaceName name of the Namespace that should be created and prepared
     * @param collectorElement the collector element
     */
    public static void createNamespaceAndPrepare(String namespaceName, CollectorElement collectorElement) {
        createNamespaceAndAddToSet(namespaceName, collectorElement);
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

    private static String getStoreName(CollectorElement collectorElement) {
        return collectorElement.testClassName();
    }

    /**
     * Add namespace to set.
     *
     * @param namespaceName the namespace name
     * @param collectorElement the collector element
     */
    public static synchronized void addNamespaceToSet(String namespaceName, CollectorElement collectorElement) {
        String storeName = getStoreName(collectorElement);
        if (!isNamespaceStored(storeName, namespaceName)) {
            Set<String> namespacesList = getNamespacesFromStore(storeName);
            namespacesList.add(namespaceName);
            getStore(storeName).put(NAMESPACES_KEY, namespacesList);
        }
    }

    private static boolean isNamespaceStored(String storeName, String namespace) {
        Set<String> namespaces = getNamespacesFromStore(storeName);
        return namespaces.contains(namespace);
    }

    private static Set<String> getNamespacesFromStore(String storeName) {
        Set<String> namespaces = getStore(storeName).get(NAMESPACES_KEY, Set.class);
        return namespaces != null ? namespaces : new HashSet<>();
    }

    private static synchronized void deleteNamespaceFromSet(String namespaceName, CollectorElement collectorElement) {
        String storeName = getStoreName(collectorElement);
        Set<String> namespaceList = getNamespacesFromStore(storeName);
        namespaceList.remove(namespaceName);
        getStore(storeName).put(NAMESPACES_KEY, namespaceList);
    }

    /**
     * For all entries inside the store it deletes all Namespaces in the particular Set
     * After that, it clears the whole Map
     * It is used mainly in {@code AbstractST.afterAllMayOverride} to remove everything after all test cases are executed
     */
    public static void deleteAllNamespacesFromSet() {
        final String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();
        String storeName = getStoreName(new CollectorElement(testSuiteName, ""));
        Set<String> namespaceList = getNamespacesFromStore(storeName);
        namespaceList.forEach(NamespaceUtils::deleteNamespaceWithWait);
        if(!namespaceList.isEmpty()) {
            getStore(storeName).remove(NAMESPACES_KEY);
        }
    }

    /**
     * Deletes Namespace with {@param namespaceName}, waits for its deletion, and in case that {@param collectorElement}
     * is not {@code null}, removes the Namespace from the store.
     *
     * @param namespaceName Name of the Namespace that should be deleted
     * @param collectorElement Collector element for removing the Namespace from the set
     */
    public static void deleteNamespaceWithWaitAndRemoveFromSet(String namespaceName, CollectorElement collectorElement) {
        deleteNamespaceWithWait(namespaceName);

        if (collectorElement != null) {
            deleteNamespaceFromSet(namespaceName, collectorElement);
        }
    }

    /**
     * Method for creating Namespace with {@param namespaceName}, waiting for its creation, and adding it
     * to the store.
     * The last step is done only in case that {@param collectorElement} is not {@code null}
     *
     * @param namespaceName name of Namespace that should be created and added to the Set
     * @param collectorElement "key" for accessing the particular Set of Namespaces
     */
    public static void createNamespaceAndAddToSet(String namespaceName, CollectorElement collectorElement) {
        createNamespaceWithWait(namespaceName);

        if (collectorElement != null) {
            addNamespaceToSet(namespaceName, collectorElement);
        }
    }

    /**
     * This method returns all Namespaces that are created for particular test-class and test-case.
     *
     * @param testClass name of the test-class where the test-case is running (for test-class-wide Namespaces)
     * @param testCase name of the test-case (for test-case Namespaces)
     * @return list of Namespaces for the test-class and test-case
     */
    public static Set<String> getListOfNamespacesForTestClassAndTestCase(String testClass, String testCase) {
        Set<String> namespaces = getNamespacesFromStore(getStoreName(new CollectorElement(testClass, "")));

        if (testCase != null) {
            Set<String> namespacesForTestCase = getNamespacesFromStore(getStoreName(new CollectorElement(testClass, testCase)));
            namespaces.addAll(namespacesForTestCase);
        }

        return namespaces;
    }
}
