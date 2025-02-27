/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private static final Map<CollectorElement, Set<String>> MAP_WITH_SUITE_NAMESPACES = new HashMap<>();

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
            LOGGER.warn("Namespace was already created!");
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
     */
    public static void createNamespaceAndPrepare(String namespaceName, CollectorElement collectorElement) {
        createNamespaceAndAddToSet(namespaceName, collectorElement);
        DeploymentUtils.registryCredentialsSecret(namespaceName);
    }

    /**
     * @return {@link #MAP_WITH_SUITE_NAMESPACES}
     */
    public static Map<CollectorElement, Set<String>> getMapWithSuiteNamespaces() {
        return MAP_WITH_SUITE_NAMESPACES;
    }

    /**
     * Adds Namespace with {@param namespaceName} to the {@code MAP_WITH_SUITE_NAMESPACES} based on the {@param collectorElement}
     * The Map of these Namespaces is then used in LogCollector for logs collection or in
     * {@code AbstractST.afterAllMayOverride} method for deleting all Namespaces after all test cases
     *
     * @param namespaceName name of the Namespace that should be added into the Set
     * @param collectorElement "key" for accessing the particular Set of Namespaces
     */
    public static synchronized void addNamespaceToSet(String namespaceName, CollectorElement collectorElement) {
        if (MAP_WITH_SUITE_NAMESPACES.containsKey(collectorElement)) {
            Set<String> testSuiteNamespaces = MAP_WITH_SUITE_NAMESPACES.get(collectorElement);
            testSuiteNamespaces.add(namespaceName);
            MAP_WITH_SUITE_NAMESPACES.put(collectorElement, testSuiteNamespaces);
        }
        else {
            // test-suite is new
            MAP_WITH_SUITE_NAMESPACES.put(collectorElement, new HashSet<>(Set.of(namespaceName)));
        }

        LOGGER.trace("SUITE_NAMESPACE_MAP: {}", MAP_WITH_SUITE_NAMESPACES);
    }

    /**
     * Removes Namespace with {@param namespaceName} from the {@link #MAP_WITH_SUITE_NAMESPACES} based on the {@param collectorElement}.
     * After the Namespace is deleted, it is removed also from the Set -> so we know that we should not collect logs from there and that
     * everything should be cleared after a test case
     *
     * @param namespaceName name of the Namespace that should be added into the Set
     * @param collectorElement "key" for accessing the particular Set of Namespaces
     */
    private static synchronized void removeNamespaceFromSet(String namespaceName, CollectorElement collectorElement) {
        // dynamically removing from the map
        Set<String> testSuiteNamespaces = new HashSet<>(MAP_WITH_SUITE_NAMESPACES.get(collectorElement));
        testSuiteNamespaces.remove(namespaceName);

        MAP_WITH_SUITE_NAMESPACES.put(collectorElement, testSuiteNamespaces);

        LOGGER.trace("SUITE_NAMESPACE_MAP after deletion: {}", MAP_WITH_SUITE_NAMESPACES);
    }

    /**
     * For all entries inside the {@link #MAP_WITH_SUITE_NAMESPACES} it deletes all Namespaces in the particular Set
     * After that, it clears the whole Map
     * It is used mainly in {@code AbstractST.afterAllMayOverride} to remove everything after all test cases are executed
     */
    public static void deleteAllNamespacesFromSet() {
        MAP_WITH_SUITE_NAMESPACES.values()
                .forEach(setOfNamespaces -> setOfNamespaces.parallelStream()
                        .forEach(NamespaceUtils::deleteNamespaceWithWait));

        MAP_WITH_SUITE_NAMESPACES.clear();
    }

    /**
     * Deletes Namespace with {@param namespaceName}, waits for its deletion, and in case that {@param collectorElement}
     * is not {@code null}, removes the Namespace from the {@link #MAP_WITH_SUITE_NAMESPACES}.
     *
     * @param namespaceName     Name of the Namespace that should be deleted
     * @param collectorElement  Collector element for removing the Namespace from the set
     */
    public static void deleteNamespaceWithWaitAndRemoveFromSet(String namespaceName, CollectorElement collectorElement) {
        deleteNamespaceWithWait(namespaceName);

        if (collectorElement != null) {
            removeNamespaceFromSet(namespaceName, collectorElement);
        }
    }

    /**
     * Method for creating Namespace with {@param namespaceName}, waiting for its creation, and adding it
     * to the {@link #MAP_WITH_SUITE_NAMESPACES}.
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
     * @param testClass     name of the test-class where the test-case is running (for test-class-wide Namespaces)
     * @param testCase      name of the test-case (for test-case Namespaces)
     *
     * @return  list of Namespaces for the test-class and test-case
     */
    public static List<String> getListOfNamespacesForTestClassAndTestCase(String testClass, String testCase) {
        List<String> namespaces = new ArrayList<>(getMapWithSuiteNamespaces().get(new CollectorElement(testClass)));

        if (testCase != null) {
            Set<String> namespacesForTestCase = getMapWithSuiteNamespaces().get(new CollectorElement(testClass, testCase));

            if (namespacesForTestCase != null) {
                namespaces.addAll(getMapWithSuiteNamespaces().get(new CollectorElement(testClass, testCase)));
            }
        }

        return namespaces;
    }
}
