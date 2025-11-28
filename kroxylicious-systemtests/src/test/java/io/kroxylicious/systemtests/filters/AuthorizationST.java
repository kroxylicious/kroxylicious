/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.filters;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;

import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.systemtests.AbstractST;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;

class AuthorizationST extends AbstractST {
    protected static final String BROKER_NODE_NAME = "kafka";
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationST.class);
    private static final String MESSAGE = "Hello-world";
    private final String clusterName = "my-cluster";
    private String bootstrap;
    private KroxyliciousOperator kroxyliciousOperator;
    private static Kroxylicious kroxylicious;
    private Map<String, String> usernamePasswords = new HashMap<>();
    List<String> aclRules;

    @BeforeAll
    void setUp() {
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
        }
        else {
            LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();

            int numberOfBrokers = 1;
            KafkaBuilder kafka = KafkaTemplates.kafkaPersistentWithAuthentication(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, numberOfBrokers);

            resourceManager.createResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.kafkaBasedNodePoolWithDualRole(BROKER_NODE_NAME, kafka.build(), numberOfBrokers),
                    kafka);
        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }

    @BeforeEach
    void beforeEach() {
        bootstrap = null;
        usernamePasswords = new HashMap<>();
        aclRules = new ArrayList<>();
        generatePasswordForNewUser(Constants.KROXYLICIOUS_ADMIN_USER);
        aclRules.add(generateAllowAclRule(Constants.KROXYLICIOUS_ADMIN_USER, "*"));
    }

    @AfterAll
    void cleanUp() {
        kroxyliciousOperator.delete();
    }

    @Test
    void produceAndConsumeMessage(String namespace) {
        int numberOfMessages = 1;
        String user = "bob"; // name shall be always lowercase
        generatePasswordForNewUser(user);
        aclRules.add(generateAllowAclRule(user, topicName));

        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        kroxylicious = new Kroxylicious(namespace);
        kroxylicious.deployPortIdentifiesNodeWithAuthorizationFilter(clusterName, usernamePasswords, aclRules);
        bootstrap = kroxylicious.getBootstrap(clusterName);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopicWithAuthentication(namespace, topicName, bootstrap, 1, 1, usernamePasswords);

        Map<String, String> additionalKafkaProps = KroxyliciousSteps.getAdditionalKafkaProps(namespace, user, usernamePasswords.get(user));
        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages, additionalKafkaProps);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2), additionalKafkaProps);
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    private String generateAllowAclRule(String userName, String topicName) {
        return generateAclRule(Decision.ALLOW, userName, topicName);
    }

    private String generateDenyAclRule(String userName, String topicName) {
        return generateAclRule(Decision.DENY, userName, topicName);
    }

    private String generateAclRule(Decision decision, String userName, String topicName) {
        String comparison = "=";
        if (topicName.contains("*")) {
            comparison = "like";
        }
        return decision.name().toLowerCase(Locale.ROOT) + " User with name = \"" + userName.toLowerCase(Locale.ROOT) + "\" to * Topic with name " + comparison + " \""
                + topicName + "\";";
    }

    private void generatePasswordForNewUser(String user) {
        String password = UUID.randomUUID().toString().replace("-", "");
        usernamePasswords.putIfAbsent(user, password);
    }
}
