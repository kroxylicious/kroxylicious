/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.io.IOException;
import java.util.Collections;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.installation.kroxy.CertManager;
import io.kroxylicious.systemtests.installation.kroxy.Kroxy;
import io.kroxylicious.systemtests.installation.strimzi.Strimzi;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Abstract st.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractST.class);

    /**
     * The constant cluster.
     */
    protected static KubeClusterResource cluster;
    /**
     * The constant strimziOperator.
     */
    protected static Strimzi strimziOperator;
    /**
     * The constant kroxy.
     */
    protected static Kroxy kroxy;

    /**
     * The constant certManager.
     */
    protected static CertManager certManager;
    /**
     * The Resource manager.
     */
    protected final ResourceManager resourceManager = ResourceManager.getInstance();

    /**
     * Before each test.
     *
     * @param testInfo the test info
     * @throws IOException the io exception
     */
    @BeforeEach
    void beforeEachTest(TestInfo testInfo) throws IOException {
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        LOGGER.info(String.format("%s.%s - STARTED", testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName()));
    }

    /**
     * Sets up the tests.
     *
     * @throws IOException the io exception
     */
    @BeforeAll
    static void setup() throws IOException {
        cluster = KubeClusterResource.getInstance();

        // simple teardown before all tests
        if (kubeClient().getNamespace(Constants.KROXY_DEFAULT_NAMESPACE) != null) {
            NamespaceUtils.deleteNamespaceWithWait(Constants.KROXY_DEFAULT_NAMESPACE);
        }
        if (kubeClient().getNamespace(Constants.CERT_MANAGER_NAMESPACE) != null) {
            NamespaceUtils.deleteNamespaceWithWait(Constants.CERT_MANAGER_NAMESPACE);
        }

        kubeClient().createNamespace(Constants.KROXY_DEFAULT_NAMESPACE);

        strimziOperator = new Strimzi(Constants.KROXY_DEFAULT_NAMESPACE);
        strimziOperator.deploy();

        certManager = new CertManager();
        certManager.deploy();
    }

    /**
     * Teardown.
     *
     * @param testInfo the test info
     * @throws IOException the io exception
     */
    @AfterAll
    static void teardown(TestInfo testInfo) throws IOException {
        strimziOperator.delete();
        kroxy.delete(testInfo);
        certManager.delete();
        NamespaceUtils.deleteNamespaceWithWait(Constants.KROXY_DEFAULT_NAMESPACE);
        NamespaceUtils.deleteNamespaceWithWait(Constants.CERT_MANAGER_NAMESPACE);
    }

    /**
     * After each test.
     *
     * @param testInfo the test info
     */
    @AfterEach
    void afterEachTest(TestInfo testInfo) {
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        LOGGER.info(String.format("%s.%s - FINISHED", testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName()));
        resourceManager.deleteResources(testInfo);
    }
}
