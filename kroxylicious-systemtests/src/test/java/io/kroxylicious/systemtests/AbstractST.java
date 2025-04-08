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
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.skodjob.testframe.listeners.TestVisualSeparatorExtension;
import io.skodjob.testframe.utils.LoggerUtils;

import io.kroxylicious.systemtests.extensions.KroxyliciousExtension;
import io.kroxylicious.systemtests.installation.strimzi.Strimzi;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

/**
 * The type Abstract st.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestVisualSeparatorExtension.class)
@ExtendWith(KroxyliciousExtension.class)
class AbstractST {
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
     */
    @BeforeEach
    void beforeEachTest() {
        topicName = "my-topic-" + UUID.randomUUID().toString().replace("-", "").substring(0, 6);
    }

    /**
     * Sets up the tests.
     */
    @BeforeAll
    static void setup() {
        cluster = KubeClusterResource.getInstance();
        strimziOperator = new Strimzi(Environment.STRIMZI_NAMESPACE);

        NamespaceUtils.createNamespaceAndPrepare(Constants.KAFKA_DEFAULT_NAMESPACE);
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
        LoggerUtils.logSeparator("#", 76);
        LOGGER.info("{} Test Suite - FINISHED", testInfo.getTestClass().get().getName());
    }

    /**
     * After each test.
     *
     * @param testInfo the test info
     */
    @AfterEach
    void afterEachTest(TestInfo testInfo) {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("———————————— {}@After Each - Clean up after test ————————————", testInfo.getTestMethod().get().getName());
    }
}
