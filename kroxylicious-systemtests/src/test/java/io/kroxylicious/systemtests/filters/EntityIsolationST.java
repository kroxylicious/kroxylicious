/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.filters;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;

import io.kroxylicious.filter.entityisolation.EntityIsolation;
import io.kroxylicious.systemtests.AbstractSystemTests;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.enums.KafkaClientType;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertAll;

public class EntityIsolationST extends AbstractSystemTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(EntityIsolationST.class);
    private static final String MESSAGE = "Hello-world";
    private final String clusterName = "entity-isolation-st-cluster";
    private String bootstrap;
    private KroxyliciousOperator kroxyliciousOperator;
    private static Kroxylicious kroxylicious;
    private final Map<String, String> usernamePasswords = new HashMap<>();

    @BeforeAll
    void setUp() {
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
        }
        else {
            LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();

            int kafkaReplicas = 1;
            resourceManager.createResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.poolWithDualRoleAndPersistentStorage(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas),
                    KafkaTemplates.kafkaWithAuthentication(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas));
        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();

        generatePasswordForNewUser(Constants.KROXYLICIOUS_ADMIN_USER);
    }

    private void generatePasswordForNewUser(String user) {
        String password = UUID.randomUUID().toString().replace("-", "");
        usernamePasswords.putIfAbsent(user, password);
    }

    @BeforeEach
    void beforeEach() {
        bootstrap = null;
    }

    @AfterAll
    void cleanUp() {
        if (kroxyliciousOperator != null) {
            kroxyliciousOperator.delete();
        }
    }

    @Test
    void testGroupId(String namespace) {
        // kcat does not support scram-sha-512 authentication: https://github.com/edenhill/kcat/issues/462
        assumeThat(Environment.KAFKA_CLIENT).isNotEqualToIgnoringCase(KafkaClientType.KCAT.name());

        int numberOfMessages = 1;
        String userBob = "bob"; // name shall be always lowercase
        String userAlice = "alice";
        generatePasswordForNewUser(userBob);
        generatePasswordForNewUser(userAlice);

        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortIdentifiesNodeWithEntityIsolationFilter(clusterName, usernamePasswords, Set.of(EntityIsolation.EntityType.GROUP_ID));
        bootstrap = kroxylicious.getBootstrap(clusterName);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopicWithAuthentication(namespace, topicName, bootstrap, 1, 1, usernamePasswords);

        Map<String, String> bobKafkaProps = KroxyliciousSteps.getAdditionalSaslProps(namespace, userBob, usernamePasswords.get(userBob));
        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages, bobKafkaProps);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        Map<String, String> aliceKafkaProps = KroxyliciousSteps.getAdditionalSaslProps(namespace, userAlice, usernamePasswords.get(userAlice));
        List<ConsumerRecord> aliceResult = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2), aliceKafkaProps);
        LOGGER.atInfo().setMessage("Received: {}").addArgument(aliceResult).log();

        List<ConsumerRecord> bobResult = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2), bobKafkaProps);
        LOGGER.atInfo().setMessage("Received: {}").addArgument(bobResult).log();

        assertAll( () -> {
            assertThat(aliceResult).withFailMessage("expected messages have not been received!")
                    .extracting(ConsumerRecord::getPayload)
                    .hasSize(numberOfMessages)
                    .allSatisfy(v -> assertThat(v).contains(MESSAGE));

            assertThat(bobResult).withFailMessage("expected messages have not been received!")
                    .extracting(ConsumerRecord::getPayload)
                    .hasSize(numberOfMessages)
                    .allSatisfy(v -> assertThat(v).contains(MESSAGE));

            assertThat(aliceResult).withFailMessage("Alice and Bob received different messages!")
                    .isEqualTo(bobResult);

            assertThat(getConsumerGroups()).withFailMessage("")
                    .hasSize(2)
                    .allSatisfy(v -> assertThat(v).endsWith(Constants.CONSUMER_GROUP_NAME))
                    .anySatisfy(v -> assertThat(v).startsWith(userBob))
                    .anySatisfy(v -> assertThat(v).startsWith(userAlice));
        });
    }

    private List<String> getConsumerGroups() {
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        String kafkaPodName = kafkaPods.stream().filter(p -> p.getMetadata().getName().contains("kafka")).findFirst().get().getMetadata().getName();
        String kafkaBootstrap = clusterName + "-kafka-bootstrap." + Constants.KAFKA_DEFAULT_NAMESPACE + ".svc.cluster.local:9094";
        List<String> command = List.of("/bin/bash", "./bin/kafka-consumer-groups.sh", "--bootstrap-server", kafkaBootstrap, "--list");
        ExecResult result = cmdKubeClient(Constants.KAFKA_DEFAULT_NAMESPACE).execInPod(kafkaPodName, true, command);
        LOGGER.info("Consumer groups: {}", result.out());
        return Arrays.stream(result.out().split("\n")).toList();
    }
}
