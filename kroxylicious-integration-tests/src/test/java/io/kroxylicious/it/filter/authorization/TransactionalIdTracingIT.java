/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.testing.kafka.junit5ext.Name;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

/**
 * A test that runs through a sequence of interactions using real high level Clients,
 * recording the outcomes or (expected) exceptions. This test isolates Transactional ID authorization
 * and does not aim to test authorization of other entities.
 * We expect the client to experience the same outcomes when pointed at Kafka-with-ACLs,
 * or proxy-with-authz (give or take some internal identifiers).
 */
class TransactionalIdTracingIT extends AbstractTracingIT {

    public static final String TOPIC_A = "topicA";
    public static final String TRANSACTIONAL_ID = "a-transaction";
    public static final String GROUP_1 = "group1";
    public static final IntegerSerializer INTEGER_SERIALIZER = new IntegerSerializer();
    public static final IntegerDeserializer INTEGER_DESERIALIZER = new IntegerDeserializer();
    public static final String TRANSACTION_PARTICIPANT = ALICE;
    public static final String TRANSACTION_ADMIN = BOB;
    public static final String UNAUTHORIZED = EVE;

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    private Path rulesFile;

    private List<AclBinding> aclBindings;

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        // implicitly all other Resource types like Topics will be allowed, because they are not imported
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TransactionalIdResource as TxnlId;
                allow User with name = "%s" to * TxnlId with name = "%s";
                allow User with name = "%s" to DESCRIBE TxnlId with name = "%s";
                otherwise deny;
                """.formatted(TRANSACTION_PARTICIPANT, TRANSACTIONAL_ID, TRANSACTION_ADMIN, TRANSACTIONAL_ID));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TRANSACTIONAL_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + TRANSACTION_PARTICIPANT, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TRANSACTIONAL_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + TRANSACTION_ADMIN, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_A, PatternType.LITERAL),
                        new AccessControlEntry("User:" + TRANSACTION_PARTICIPANT, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_1, PatternType.LITERAL),
                        new AccessControlEntry("User:" + TRANSACTION_PARTICIPANT, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_A, PatternType.LITERAL),
                        new AccessControlEntry("User:" + TRANSACTION_ADMIN, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_A, PatternType.LITERAL),
                        new AccessControlEntry("User:" + UNAUTHORIZED, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_1, PatternType.LITERAL),
                        new AccessControlEntry("User:" + TRANSACTION_ADMIN, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW))

        );
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(), aclBindings);
    }

    @AfterEach
    void tidyClusters() {
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(TOPIC_A), aclBindings);
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(TOPIC_A), List.of());
    }

    static class UnauthorizedProducer implements Prog {

        private final String actorName;
        private final String beginTransactionErrorMessage;
        private final Class<?> beginTransactionErrorClass;

        UnauthorizedProducer(String actorName, String beginTransactionErrorMessage, Class<?> beginTransactionErrorClass) {
            this.actorName = actorName;
            this.beginTransactionErrorMessage = beginTransactionErrorMessage;
            this.beginTransactionErrorClass = beginTransactionErrorClass;
        }

        @Override
        @SuppressWarnings("java:S2925") // Thread.sleep
        public void run(ClientFactory clientFactory) {
            String topicA = TOPIC_A;
            String setupUser = actorName;
            String producerUser = actorName;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"))) {
                setup.createTopic(topicA);
                try (var transactionalProducer = clientFactory.newProducer(producerUser, INTEGER_SERIALIZER, Map.of(
                        ProducerConfig.CLIENT_ID_CONFIG, "transactionalProducer",
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID,
                        ProducerConfig.PARTITIONER_CLASS_CONFIG, AlwaysPartition0Partitioner.class.getName()))) {

                    transactionalProducer.initTransactions();

                    // completion of initTransactions is driven before the internal state of the txnManager is transitioned
                    // so the client can observe different exceptions if we don't wait for the state transition
                    Awaitility.await().untilAsserted(() -> assertThatThrownBy(() -> {
                        transactionalProducer.producer().beginTransaction();
                    }).isInstanceOf(beginTransactionErrorClass)
                            .hasMessage(beginTransactionErrorMessage));

                    // Commit
                    transactionalProducer.beginTransaction();
                    transactionalProducer.send(topicA, 2);
                    transactionalProducer.commitTransaction();

                    // Abort
                    transactionalProducer.beginTransaction();
                    transactionalProducer.send(topicA, 3);
                    transactionalProducer.abortTransaction();

                    // Commit
                    transactionalProducer.beginTransaction();
                    transactionalProducer.send(topicA, 4);
                    transactionalProducer.commitTransaction();
                }
                setup.deleteTopic(topicA);
            }
        }
    }

    static class AdminProg implements Prog {

        private final String setupActorName;
        private final String adminActorName;
        private final boolean expectAllowedToListAndDescribe;

        AdminProg(String adminActorName, String setupActorName, boolean expectAllowedToListAndDescribe) {
            // user permitted to create topics and participate in the transaction
            this.setupActorName = setupActorName;
            // user with permission to describe the transaction
            this.adminActorName = adminActorName;
            this.expectAllowedToListAndDescribe = expectAllowedToListAndDescribe;
        }

        @Override
        @SuppressWarnings("java:S2925") // Thread.sleep
        public void run(ClientFactory clientFactory) {
            String topicA = TOPIC_A;

            try (var setup = clientFactory.newAdmin(setupActorName, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"))) {
                setup.createTopic(topicA);
                try (var transactionalProducer = clientFactory.newProducer(setupActorName, INTEGER_SERIALIZER, Map.of(
                        ProducerConfig.CLIENT_ID_CONFIG, "transactionalProducer",
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID,
                        ProducerConfig.PARTITIONER_CLASS_CONFIG, AlwaysPartition0Partitioner.class.getName()))) {

                    transactionalProducer.initTransactions();

                    // Commit
                    transactionalProducer.beginTransaction();
                    transactionalProducer.send(topicA, 2);
                    Outcome<Void> voidOutcome = transactionalProducer.commitTransaction();
                    assertThat(voidOutcome.isSuccess()).isTrue();
                }
                try (var admin = clientFactory.newAdmin(adminActorName, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "admin"))) {
                    Outcome<TransactionDescription> describeTransaction = admin.describeTransaction(TRANSACTIONAL_ID);
                    Outcome<Collection<TransactionListing>> listTransactions = admin.listTransactions();
                    assertThat(listTransactions.isSuccess()).isTrue();
                    Stream<String> actualListedIds = listTransactions.value().stream().map(TransactionListing::transactionalId);
                    if (expectAllowedToListAndDescribe) {
                        assertThat(describeTransaction.isSuccess()).isTrue();
                        assertThat(describeTransaction.value().state()).isEqualTo(TransactionState.COMPLETE_COMMIT);
                        assertThat(actualListedIds).contains(TRANSACTIONAL_ID);
                    }
                    else {
                        assertThat(describeTransaction.isSuccess()).isFalse();
                        assertThat(actualListedIds).doesNotContain(TRANSACTIONAL_ID);
                    }
                }
                setup.deleteTopic(topicA);
            }
        }
    }

    static class TransactionalProg implements Prog {

        @Override
        @SuppressWarnings("java:S2925") // Thread.sleep
        public void run(ClientFactory clientFactory) throws InterruptedException {
            String topicA = TOPIC_A;
            TopicPartition topicAPartition0 = new TopicPartition(topicA, 0);

            String setupUser = TRANSACTION_PARTICIPANT;
            String producerUser = TRANSACTION_PARTICIPANT;
            String consumerUser = TRANSACTION_PARTICIPANT;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"))) {
                setup.createTopic(topicA);
                try (var transactionalProducer = clientFactory.newProducer(producerUser, INTEGER_SERIALIZER, Map.of(
                        ProducerConfig.CLIENT_ID_CONFIG, "transactionalProducer",
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID,
                        ProducerConfig.PARTITIONER_CLASS_CONFIG, AlwaysPartition0Partitioner.class.getName()))) {

                    transactionalProducer.initTransactions();

                    // Commit
                    transactionalProducer.beginTransaction();
                    transactionalProducer.send(topicA, 2);
                    transactionalProducer.commitTransaction();

                    // Abort
                    transactionalProducer.beginTransaction();
                    transactionalProducer.send(topicA, 3);
                    transactionalProducer.abortTransaction();

                    // Commit
                    transactionalProducer.beginTransaction().value();
                    var sent = transactionalProducer.send(topicA, 4).value().offset();
                    transactionalProducer.commitTransaction().value();

                    try (var consumer2 = clientFactory.newConsumer(consumerUser, INTEGER_DESERIALIZER, Map.of(
                            ConsumerConfig.CLIENT_ID_CONFIG, "consumer2",
                            ConsumerConfig.GROUP_ID_CONFIG, GROUP_1,
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, GROUP_1 + "-instance-1",
                            ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1))) {

                        consumer2.subscribe(TOPIC_A);
                        ConsumerRecords<String, Integer> records;
                        Instant start = Instant.now();
                        while (true) {
                            if (Instant.now().minusSeconds(10).isAfter(start)) {
                                throw new RuntimeException("timed out waiting for records");
                            }
                            records = consumer2.poll().value();
                            List<ConsumerRecord<String, Integer>> records1 = records.records(topicAPartition0);
                            if (!records1.isEmpty()
                                    && records1.get(records1.size() - 1).offset() >= sent) {
                                break;
                            }
                            Thread.sleep(100);
                        }

                        var offsets = records.nextOffsets();
                        assertThat(offsets).isNotEmpty();
                        assertThat(offsets.get(topicAPartition0).offset()).isGreaterThan(0);
                        var metadata = consumer2.groupMetadata().value();
                        assertThat(metadata.generationId()).isGreaterThan(0);
                        transactionalProducer.beginTransaction();
                        assertThat(transactionalProducer.sendOffsetsToTransaction(offsets, metadata).value()).isNull();
                        transactionalProducer.commitTransaction();
                    }
                }
                setup.deleteTopic(topicA);
            }
        }
    }

    List<Arguments> shouldEnforceAccessToTransactions() {
        // The transaction participant persona has write permissions for the transactional id. They are allowed to:
        // * initialize transactions
        // * send transactional records
        // * commit or abort the transaction
        // * participate in the transaction as a consumer, adding offsets to the transaction.
        // * list and describe the transaction via the Admin APIs.
        var transactionParticipant = new Actor(TRANSACTION_PARTICIPANT, PASSWORDS.get(TRANSACTION_PARTICIPANT), Map.of(), Map.of(), Map.of());

        // The transaction admin persona has read permissions for the transactional id. They are allowed to:
        // * list and describe the transaction via the Admin APIs.
        var transactionAdmin = new Actor(TRANSACTION_ADMIN, PASSWORDS.get(TRANSACTION_ADMIN), Map.of(), Map.of(), Map.of());

        // The unauthorized persona is an actor with no permissions to interact with the transactional id
        var unauthorized = new Actor(UNAUTHORIZED, PASSWORDS.get(UNAUTHORIZED), Map.of(), Map.of(), Map.of());

        List<Actor> allActors = List.of(transactionAdmin, transactionParticipant, unauthorized);
        return List.of(
                argumentSet("transaction participant can write to transactional id", new TransactionalProg(), allActors),
                // admin actor is allowed to FindCoordinator via DESCRIBE permission, so beginTransaction fails in a particular way
                argumentSet("transaction admin actor cannot produce using transactionalId", new UnauthorizedProducer(TRANSACTION_ADMIN,
                        "TransactionalId a-transaction: Invalid transition attempted from state UNINITIALIZED to state IN_TRANSACTION", IllegalStateException.class),
                        allActors),
                argumentSet("unauthorized actor cannot produce using transactionalId",
                        new UnauthorizedProducer(UNAUTHORIZED, "Cannot execute transactional method because we are in an error state", KafkaException.class),
                        List.of(unauthorized)),
                argumentSet("transaction admin actor can list and describe transaction", new AdminProg(TRANSACTION_ADMIN, TRANSACTION_PARTICIPANT, true), allActors),
                argumentSet("transaction participant actor can list and describe transaction", new AdminProg(TRANSACTION_PARTICIPANT, TRANSACTION_PARTICIPANT, true),
                        allActors),
                argumentSet("unauthorized actor cannot list or describe transaction", new AdminProg(UNAUTHORIZED, TRANSACTION_PARTICIPANT, false), allActors));
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTransactions(Prog prog, List<Actor> actors) throws Exception {
        assertProgTraceMatches(prog, actors, rulesFile);
    }

    public static class AlwaysPartition0Partitioner implements Partitioner {

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return 0;
        }

        @Override
        public void close() {
            // stateless
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // stateless
        }
    }

}
