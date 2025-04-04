/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousApp;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The Kroxylicious app system tests.
 * If using minikube, 'minikube tunnel' shall be executed before these tests
 *
 * Disabled to focus on kubernetes system tests
 */
@Disabled
class KroxyliciousAppST extends AbstractST {
    private static final Logger LOGGER = LoggerFactory.getLogger(KroxyliciousAppST.class);
    private static KroxyliciousApp kroxyliciousApp;
    private final String clusterName = "my-external-cluster";

    /**
     * Kroxylicious app is running.
     */
    @Test
    void kroxyAppIsRunning() {
        LOGGER.info("Given local Kroxylicious");
        String clusterIp = kubeClient().getService(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName + "-kafka-external-bootstrap").getSpec().getClusterIP();
        kroxyliciousApp = new KroxyliciousApp(clusterIp);
        kroxyliciousApp.waitForKroxyliciousProcess();
        assertThat(kroxyliciousApp.isRunning()).withFailMessage("Kroxylicious app is not running!").isTrue();
    }

    /**
     * Sets before all.
     */
    @BeforeAll
    void setupBefore() {
        assumeTrue(DeploymentUtils.checkLoadBalancerIsWorking(Constants.KAFKA_DEFAULT_NAMESPACE), "Load balancer is not working fine, if you are using"
                + "minikube please run 'minikube tunnel' before running the tests");
        LOGGER.info("Deploying Kafka in {} namespace", Constants.KAFKA_DEFAULT_NAMESPACE);
        resourceManager.createResourceFromBuilderWithWait(KafkaTemplates.kafkaPersistentWithExternalIp(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, 3, 3));
    }

    /**
     * Tear down.
     */
    @AfterEach
    void tearDown() {
        if (kroxyliciousApp != null) {
            LOGGER.info("Removing kroxylicious app");
            kroxyliciousApp.stop();
        }
    }
}
