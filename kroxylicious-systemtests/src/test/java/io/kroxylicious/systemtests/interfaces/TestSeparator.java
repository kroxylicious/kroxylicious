/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.interfaces;

import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.kroxylicious.systemtests.extensions.KroxyliciousExtension;

/**
 * Separates different tests in the log output
 */
@ExtendWith(KroxyliciousExtension.class)
public interface TestSeparator {
    /**
     * Logger used to log the separator message
     */
    Logger LOGGER = LogManager.getLogger(TestSeparator.class);
    /**
     * Separator character used in the log output
     */
    String SEPARATOR_CHAR = "#";

    /**
     * Prints the separator at the start of the test
     *
     * @param testContext   Test context
     */
    @BeforeEach
    default void beforeEachTest(ExtensionContext testContext) {
        LOGGER.info(String.join("", Collections.nCopies(76, SEPARATOR_CHAR)));
        LOGGER.info("{}.{}-STARTED", testContext.getRequiredTestClass().getName(), testContext.getRequiredTestMethod().getName());
    }

    /**
     * Prints the separator at the end of the test
     *
     * @param testContext   Test context
     */
    @AfterEach
    default void afterEachTest(ExtensionContext testContext) {
        if (testContext.getExecutionException().isPresent()) {
            LOGGER.info("{}.{}-FAILED", testContext.getRequiredTestClass().getName(), testContext.getRequiredTestMethod().getName());
        }
        else {
            LOGGER.info("{}.{}-SUCCEEDED", testContext.getRequiredTestClass().getName(), testContext.getRequiredTestMethod().getName());
        }

        LOGGER.info(String.join("", Collections.nCopies(76, SEPARATOR_CHAR)));
    }
}