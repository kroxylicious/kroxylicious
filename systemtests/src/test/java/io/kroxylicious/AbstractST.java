/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

import java.util.Collections;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.installation.strimzi.Strimzi;
import io.kroxylicious.k8s.KubeClusterResource;
import io.kroxylicious.resources.manager.ResourceManager;
import io.kroxylicious.utils.NamespaceUtils;

import static io.kroxylicious.k8s.KubeClusterResource.kubeClient;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractST.class);

    protected static KubeClusterResource cluster;
    protected static Strimzi strimziOperator;
    protected final ResourceManager resourceManager = ResourceManager.getInstance();

    @BeforeEach
    void beforeEachTest(TestInfo testInfo) {
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        LOGGER.info(String.format("%s.%s - STARTED", testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName()));
    }

    @BeforeAll
    static void setup() {
        cluster = KubeClusterResource.getInstance();

        // simple teardown before all tests
        if (kubeClient().getNamespace(Constants.STRIMZI_DEFAULT_NAMESPACE) != null) {
            NamespaceUtils.deleteNamespaceWithWait(Constants.STRIMZI_DEFAULT_NAMESPACE);
        }

        kubeClient().createNamespace(Constants.STRIMZI_DEFAULT_NAMESPACE);

        strimziOperator = new Strimzi(Constants.STRIMZI_DEFAULT_NAMESPACE);
        strimziOperator.deploy();
    }

    @AfterAll
    static void teardown() {
        strimziOperator.delete();
        NamespaceUtils.deleteNamespaceWithWait(Constants.STRIMZI_DEFAULT_NAMESPACE);
    }

    @AfterEach
    void afterEachTest(TestInfo testInfo) {
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        LOGGER.info(String.format("%s.%s - FINISHED", testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName()));
        resourceManager.deleteResources(testInfo);
    }
}
