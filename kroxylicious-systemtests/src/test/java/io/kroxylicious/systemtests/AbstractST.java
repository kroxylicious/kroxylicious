/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.installation.strimzi.Strimzi;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

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
     * The Resource manager.
     */
    protected final ResourceManager resourceManager = ResourceManager.getInstance();
    protected String topicName;

    /**
     * Before each test.
     *
     * @param testInfo the test info
     */
    @BeforeEach
    void beforeEachTest(TestInfo testInfo) {
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        LOGGER.info(String.format("%s.%s - STARTED", testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName()));
        topicName = "my-topic-" + UUID.randomUUID().toString().replace("-", "").substring(0, 6);
    }

    /**
     * Sets up the tests.
     *
     * @param testInfo the test info
     */
    @BeforeAll
    static void setup(TestInfo testInfo) {
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        LOGGER.info(String.format("%s Test Suite - STARTED", testInfo.getTestClass().get().getName()));
        cluster = KubeClusterResource.getInstance();
        strimziOperator = new Strimzi(Environment.STRIMZI_NAMESPACE);

        NamespaceUtils.createNamespaceWithWait(Constants.KAFKA_DEFAULT_NAMESPACE);
        strimziOperator.deploy();
    }

    /**
     * Teardown.
     *
     * @param testInfo the test info
     * @throws IOException the io exception
     */
    @AfterAll
    static void teardown(TestInfo testInfo) throws IOException {
        if (!Environment.SKIP_TEARDOWN) {
            if (strimziOperator != null) {
                strimziOperator.delete();
            }
        }
        else {
            LOGGER.warn("Teardown was skipped because SKIP_TEARDOWN was set to 'true'");
        }
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        LOGGER.info(String.format("%s Test Suite - FINISHED", testInfo.getTestClass().get().getName()));
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
    }
}
