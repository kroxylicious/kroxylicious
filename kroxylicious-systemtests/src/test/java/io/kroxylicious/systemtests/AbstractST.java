/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.installation.kroxy.Kroxy;
import io.kroxylicious.systemtests.installation.strimzi.Strimzi;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractST.class);

    protected static KubeClusterResource cluster;
    protected static Strimzi strimziOperator;
    protected static Kroxy kroxy;
    protected final ResourceManager resourceManager = ResourceManager.getInstance();

    @BeforeEach
    void beforeEachTest(TestInfo testInfo) {
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        LOGGER.info(String.format("%s.%s - STARTED", testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName()));
    }

    @BeforeAll
    static void setup() throws IOException {
        cluster = KubeClusterResource.getInstance();

        // simple teardown before all tests
        if (kubeClient().getNamespace(Constants.KROXY_DEFAULT_NAMESPACE) != null) {
            NamespaceUtils.deleteNamespaceWithWait(Constants.KROXY_DEFAULT_NAMESPACE);
        }
        if (kubeClient().getNamespace("cert-manager") != null) {
            NamespaceUtils.deleteNamespaceWithWait("cert-manager");
        }

        kubeClient().createNamespace(Constants.KROXY_DEFAULT_NAMESPACE);

        strimziOperator = new Strimzi(Constants.KROXY_DEFAULT_NAMESPACE);
        strimziOperator.deploy();

        // start kroxy
        Path path = Path.of(System.getProperty("user.dir")).getParent();
        kroxy = new Kroxy(Constants.KROXY_DEFAULT_NAMESPACE, path + Constants.KROXY_KUBE_DIR_PORTPERBROKER);
        kroxy.deploy();
    }

    @AfterAll
    static void teardown() throws IOException {
        strimziOperator.delete();
        kroxy.delete();
        NamespaceUtils.deleteNamespaceWithWait(Constants.KROXY_DEFAULT_NAMESPACE);
        NamespaceUtils.deleteNamespaceWithWait("cert-manager");
    }

    @AfterEach
    void afterEachTest(TestInfo testInfo) {
        LOGGER.info(String.join("", Collections.nCopies(76, "#")));
        LOGGER.info(String.format("%s.%s - FINISHED", testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName()));
        resourceManager.deleteResources(testInfo);
    }
}
