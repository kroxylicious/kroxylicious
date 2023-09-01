/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.kroxylicious.systemtest.listeners;

import java.util.Arrays;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.engine.TestTag;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import io.kroxylicious.systemtest.Constants;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ExecutionListener implements TestExecutionListener {
    private static final Logger LOGGER = LogManager.getLogger(TestExecutionListener.class);

    /* read only */ private static TestPlan testPlan;

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public void testPlanExecutionStarted(TestPlan plan) {
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
        LOGGER.info("                        Test run started");
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
        testPlan = plan;
        printSelectedTestClasses(testPlan);
    }

    public void testPlanExecutionFinished(TestPlan testPlan) {
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
        LOGGER.info("                        Test run finished");
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
    }

    private void printSelectedTestClasses(TestPlan plan) {
        LOGGER.info("Following testclasses are selected for run:");
        Arrays.asList(plan.getChildren(plan.getRoots()
                .toArray(new TestIdentifier[0])[0])
                .toArray(new TestIdentifier[0])).forEach(testIdentifier -> LOGGER.info("-> {}", testIdentifier.getLegacyReportingName()));
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
    }

    /**
     * Checks if test suite has test case, which is labeled as {@link io.kroxylicious.systemtest.annotations.ParallelTest} or
     * {@link io.kroxylicious.systemtest.annotations.IsolatedTest}.
     *
     * @param extensionContext  ExtensionContext of the test case
     * @return                  true if test suite contains Parallel or Isolated test case. Otherwise, false.
     */
    public static boolean hasSuiteParallelOrIsolatedTest(final ExtensionContext extensionContext) {
        Set<TestIdentifier> testCases = testPlan.getChildren(extensionContext.getUniqueId());

        for (TestIdentifier testIdentifier : testCases) {
            for (TestTag testTag : testIdentifier.getTags()) {
                if (testTag.getName().equals(Constants.PARALLEL_TEST) || testTag.getName().equals(Constants.ISOLATED_TEST) ||
                // Dynamic configuration also because in DynamicConfSharedST we use @TestFactory
                        testTag.getName().equals(Constants.DYNAMIC_CONFIGURATION) ||
                        // Tracing, because we deploy Jaeger operator inside additional namespace
                        testTag.getName().equals(Constants.TRACING) ||
                        // KafkaVersionsST, because here we use @ParameterizedTest
                        testTag.getName().equals(Constants.KAFKA_SMOKE)) {
                    return true;
                }
            }
        }
        return false;
    }
}
