/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.fabric8.kubernetes.api.model.batch.v1.Job;

import io.kroxylicious.systemtest.annotations.ParallelNamespaceTest;
import io.kroxylicious.systemtest.kafkaClients.internalClients.KafkaClients;
import io.kroxylicious.systemtest.storage.TestStorage;
import io.kroxylicious.systemtest.templates.crd.KafkaTemplates;
import io.kroxylicious.systemtest.templates.crd.KafkaTopicTemplates;
import io.kroxylicious.systemtest.utils.ClientUtils;
import io.kroxylicious.systemtest.utils.TestKafkaVersion;

import static io.kroxylicious.systemtest.Constants.KAFKA_TRACING_CLIENT_KEY;

public class AcceptanceST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AcceptanceST.class);

    @BeforeAll
    void setup(final ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
                .defaultInstallation(extensionContext)
                .createInstallation()
                .runInstallation();
    }

//    @ParameterizedTest(name = "Kafka version: {0}.version()")
    @ParallelNamespaceTest
    void testKafka(final TestKafkaVersion testKafkaVersion, ExtensionContext extensionContext) {
        LOGGER.info("Deploying Kafka with version: {}", testKafkaVersion.version());
        resourceManager.createResourceWithWait(extensionContext,
                KafkaTemplates.kafkaEphemeral(storageMap.get(extensionContext).getClusterName(), 3, 1)
//                        .editOrNewSpec()
//                        .editOrNewKafka()
//                        .withVersion(testKafkaVersion.version())
//                        .endKafka()
//                        .endSpec()
                        .build());

        final TestStorage testStorage = storageMap.get(extensionContext);
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 12, 3, testStorage.getNamespaceName()).build());
        // For kroxy create a new .producerKroxy() and .consumerKroxy()
        KafkaClients kafkaClient = ((KafkaClients) testStorage.retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY));
        Job producerJob = kafkaClient.producerStrimzi();
        Job consumerJob = kafkaClient.consumerStrimzi();
        resourceManager.createResourceWithWait(extensionContext, producerJob);
        resourceManager.createResourceWithWait(extensionContext, consumerJob);

        ClientUtils.waitForClientsSuccess(testStorage);
//        kafkaClient.setMessage("Blah");
//        assert kafkaClient.getMessageCount() == 1;
    }

    @AfterAll
    void CleanUp() {
        this.clusterOperator.unInstall();
    }

//    /**
//     * Test checking basic functionality for each supported Kafka version.
//     * Ensures that for every Kafka version:
//     *     - Kafka cluster is deployed without an issue
//     *       - with Topic Operator, User Operator, 3 Zookeeper and Kafka pods
//     *     - Topic Operator is working - because of the KafkaTopic creation
//     *     - User Operator is working - because of SCRAM-SHA, ACLs and overall KafkaUser creations
//     *     - Sending and receiving messages is working to PLAIN (with SCRAM-SHA) and TLS listeners
//     * @param testKafkaVersion TestKafkaVersion added for each iteration of the parametrized test
//     * @param extensionContext context in which the current test is being executed
//     */
//    @ParameterizedTest(name = "Kafka version: {0}.version()")
//    @MethodSource("io.kroxylicious.systemtest.utils.TestKafkaVersion#getSupportedKafkaVersions")
//    void testKafkaWithVersion(final TestKafkaVersion testKafkaVersion, ExtensionContext extensionContext) {
//        // skip test if KRaft mode is enabled and Kafka version is lower than 3.5.0 - https://github.com/strimzi/strimzi-kafka-operator/issues/8806
//        assumeTrue(Environment.isKRaftModeEnabled() && TestKafkaVersion.compareDottedVersions("3.5.0", testKafkaVersion.version()) != 1);
//
//        final TestStorage testStorage = new TestStorage(extensionContext);
//
//        final String kafkaUserRead = testStorage.getUsername() + "-read";
//        final String kafkaUserWrite = testStorage.getUsername() + "-write";
//        final String kafkaUserReadWriteTls = testStorage.getUsername() + "-read-write";
//        final String readConsumerGroup = ClientUtils.generateRandomConsumerGroup();
//
//        LOGGER.info("Deploying Kafka with version: {}", testKafkaVersion.version());
//
//        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
//                .editMetadata()
//                .withNamespace(testStorage.getNamespaceName())
//                .endMetadata()
//                .editOrNewSpec()
//                .editOrNewKafka()
//                .withVersion(testKafkaVersion.version())
//                .addToConfig("auto.create.topics.enable", "true")
//                .addToConfig("inter.broker.protocol.version", testKafkaVersion.protocolVersion())
//                .addToConfig("log.message.format.version", testKafkaVersion.messageVersion())
//                .withNewKafkaAuthorizationSimple()
//                .endKafkaAuthorizationSimple()
//                .withListeners(
//                        new GenericKafkaListenerBuilder()
//                                .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
//                                .withPort(9092)
//                                .withType(KafkaListenerType.INTERNAL)
//                                .withTls(false)
//                                .withNewKafkaListenerAuthenticationScramSha512Auth()
//                                .endKafkaListenerAuthenticationScramSha512Auth()
//                                .build(),
//                        new GenericKafkaListenerBuilder()
//                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
//                                .withPort(9093)
//                                .withType(KafkaListenerType.INTERNAL)
//                                .withTls(true)
//                                .withNewKafkaListenerAuthenticationTlsAuth()
//                                .endKafkaListenerAuthenticationTlsAuth()
//                                .build()
//                )
//                .endKafka()
//                .endSpec()
//                .build()
//        );
//
//        KafkaUser writeUser = KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaUserWrite)
//                .editSpec()
//                .withNewKafkaUserAuthorizationSimple()
//                .addNewAcl()
//                .withNewAclRuleTopicResource()
//                .withName(testStorage.getTopicName())
//                .endAclRuleTopicResource()
//                // we need CREATE for topic creation in Kafka (auto.create.topics.enable - true)
//                .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE, AclOperation.CREATE)
//                .endAcl()
//                .endKafkaUserAuthorizationSimple()
//                .endSpec()
//                .build();
//
//        KafkaUser readUser = KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaUserRead)
//                .editSpec()
//                .withNewKafkaUserAuthorizationSimple()
//                .addNewAcl()
//                .withNewAclRuleTopicResource()
//                .withName(testStorage.getTopicName())
//                .endAclRuleTopicResource()
//                .withOperations(AclOperation.READ, AclOperation.DESCRIBE)
//                .endAcl()
//                .addNewAcl()
//                .withNewAclRuleGroupResource()
//                .withName(readConsumerGroup)
//                .endAclRuleGroupResource()
//                .withOperations(AclOperation.READ)
//                .endAcl()
//                .endKafkaUserAuthorizationSimple()
//                .endSpec()
//                .build();
//
//        KafkaUser tlsReadWriteUser = KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), kafkaUserReadWriteTls)
//                .editSpec()
//                .withNewKafkaUserAuthorizationSimple()
//                .addNewAcl()
//                .withNewAclRuleTopicResource()
//                .withName(testStorage.getTopicName())
//                .endAclRuleTopicResource()
//                .withOperations(AclOperation.WRITE, AclOperation.READ, AclOperation.DESCRIBE)
//                .endAcl()
//                .addNewAcl()
//                .withNewAclRuleGroupResource()
//                .withName(readConsumerGroup)
//                .endAclRuleGroupResource()
//                .withOperations(AclOperation.READ)
//                .endAcl()
//                .endKafkaUserAuthorizationSimple()
//                .endSpec()
//                .build();
//
//        resourceManager.createResourceWithWait(extensionContext,
//                KafkaTopicTemplates.topic(testStorage).build(),
//                readUser,
//                writeUser,
//                tlsReadWriteUser
//        );
//
//        LOGGER.info("Sending and receiving messages via PLAIN -> SCRAM-SHA");
//
//        KafkaClients kafkaClients = new KafkaClientsBuilder()
//                .withTopicName(testStorage.getTopicName())
//                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
//                .withNamespaceName(testStorage.getNamespaceName())
//                .withProducerName(testStorage.getProducerName())
//                .withConsumerName(testStorage.getConsumerName())
//                .withMessageCount(testStorage.getMessageCount())
//                .withUsername(kafkaUserWrite)
//                .withConsumerGroup(readConsumerGroup)
//                .build();
//
//        resourceManager.createResourceWithWait(extensionContext, kafkaClients.producerScramShaPlainStrimzi());
//        ClientUtils.waitForProducerClientSuccess(testStorage);
//
//        kafkaClients = new KafkaClientsBuilder(kafkaClients)
//                .withUsername(kafkaUserRead)
//                .build();
//
//        resourceManager.createResourceWithWait(extensionContext, kafkaClients.consumerScramShaPlainStrimzi());
//        ClientUtils.waitForConsumerClientSuccess(testStorage);
//
//        LOGGER.info("Sending and receiving messages via TLS");
//
//        kafkaClients = new KafkaClientsBuilder(kafkaClients)
//                .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
//                .withUsername(kafkaUserReadWriteTls)
//                .build();
//
//        resourceManager.createResourceWithWait(extensionContext,
//                kafkaClients.producerTlsStrimzi(testStorage.getClusterName()),
//                kafkaClients.consumerTlsStrimzi(testStorage.getClusterName())
//        );
//
//        ClientUtils.waitForClientsSuccess(testStorage);
//    }
//
//    void doTestProducerConsumerStreamsService(final ExtensionContext extensionContext) {
//        // Current implementation of Jaeger deployment and test parallelism does not allow to run this test with STRIMZI_RBAC_SCOPE=NAMESPACE`
//        assumeFalse(Environment.isNamespaceRbacScope());
//
//        resourceManager.createResourceWithWait(extensionContext,
//                KafkaTemplates.kafkaEphemeral(
//                        storageMap.get(extensionContext).getClusterName(), 3, 1).build());
//
//        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(), storageMap.get(extensionContext).getTopicName(), 12, 3, storageMap.get(extensionContext).getNamespaceName()).build());
//
//        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(storageMap.get(extensionContext).getClusterName(), storageMap.get(extensionContext).retrieveFromTestStorage(Constants.STREAM_TOPIC_KEY).toString(), 12, 3, storageMap.get(extensionContext).getNamespaceName()).build());
//
//        resourceManager.createResourceWithWait(extensionContext, ((KafkaTracingClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).producerWithTracing());
//
//        AcceptanceUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
//                JAEGER_PRODUCER_SERVICE,
//                storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(),
//                JAEGER_QUERY_SERVICE);
//
//        resourceManager.createResourceWithWait(extensionContext, ((KafkaTracingClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).consumerWithTracing());
//
//        AcceptanceUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
//                JAEGER_CONSUMER_SERVICE,
//                storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(),
//                JAEGER_QUERY_SERVICE);
//
//        resourceManager.createResourceWithWait(extensionContext, ((KafkaTracingClients) storageMap.get(extensionContext).retrieveFromTestStorage(KAFKA_TRACING_CLIENT_KEY)).kafkaStreamsWithTracing());
//
//        // Disabled for OpenTracing, because of issue with Streams API and tracing https://github.com/strimzi/strimzi-kafka-operator/issues/5680
//        if (this.getClass().getSimpleName().contains(TracingConstants.OPEN_TELEMETRY)) {
//            AcceptanceUtils.verify(storageMap.get(extensionContext).getNamespaceName(),
//                    JAEGER_KAFKA_STREAMS_SERVICE,
//                    storageMap.get(extensionContext).retrieveFromTestStorage(Constants.SCRAPER_POD_KEY).toString(),
//                    JAEGER_QUERY_SERVICE);
//        }
//    }
}
