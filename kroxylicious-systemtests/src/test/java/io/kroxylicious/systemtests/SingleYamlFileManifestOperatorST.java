/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import org.apache.kafka.common.record.CompressionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.skodjob.kubetest4j.installation.InstallationMethod;

import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousBuilder;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.resources.operator.AllInOneYamlManifestProvider;
import io.kroxylicious.systemtests.resources.operator.KroxyliciousOperatorYamlInstaller;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.TestTags.OPERATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * System tests that verify operator installation using single YAML file manifest.
 * Tests the all-in-one manifest approach for installation.
 */
@Tag(OPERATOR)
class SingleYamlFileManifestOperatorST extends AbstractSystemTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleYamlFileManifestOperatorST.class);
    private static final String TOPIC_NAME = "single-yaml-test-topic";
    private static final String MESSAGE = "single-yaml-test-message";
    private static final String clusterName = "kroxylicious-st-cluster";

    @BeforeAll
    void setupBeforeSingleYamlFileTest() {
        var manifestFile = Path.of("target/kroxylicious-operator-install.yaml");
        assumeThat(manifestFile)
                .describedAs("Single YAML file manifest should exist")
                .exists()
                .isRegularFile();

        KroxyliciousOperator operator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE) {
            @Override
            protected InstallationMethod createInstallationMethod() {
                return new KroxyliciousOperatorYamlInstaller(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE, Map.of(), new AllInOneYamlManifestProvider(manifestFile));
            }
        };
        operator.deploy();

        resourceManager.createOrUpdateResourceFromBuilderWithWait(
                KafkaNodePoolTemplates.poolWithDualRoleAndPersistentStorage(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, 1),
                KafkaTemplates.defaultKafka(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, 1));

    }

    @AfterAll
    void cleanupAfterSingleYamlFileTest() {
        LOGGER.atInfo().log("Cleaning up single YAML file test resources");
        NamespaceUtils.deleteNamespaceWithWait(Constants.KROXYLICIOUS_NAMESPACE);

    }

    @Test
    void shouldProduceAndConsumeWithOperatorFromSingleYamlFile(String namespace) {
        // Given
        var kroxylicious = KroxyliciousBuilder.singleNodeBaseBuilder(namespace,
                clusterName, 1).build();
        kroxylicious.createOrUpdateResources();

        var bootstrap = kroxylicious.getBootstrap(namespace, clusterName);

        KafkaSteps.createTopic(namespace, TOPIC_NAME, bootstrap, 1, 1, CompressionType.NONE);

        KroxyliciousSteps.produceMessages(namespace, TOPIC_NAME, bootstrap, MESSAGE, CompressionType.NONE, 1);

        // When
        var result = KroxyliciousSteps.consumeMessages(namespace, TOPIC_NAME, bootstrap, 1, Duration.ofMinutes(2));

        // Then
        assertThat(result)
                .describedAs("Message should be received from proxy deployed by operator from single YAML file manifest")
                .hasSize(1)
                .anySatisfy(record -> assertThat(record.getPayload()).contains(MESSAGE));
    }
}
