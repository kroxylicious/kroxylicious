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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
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

/**
 * A test that runs through a sequence of interactions using real high level Clients,
 * recording the outcomes or (expected) exceptions. This test isolates Topic authorization
 * and does not aim to test authorization of other entities.
 * We expect the client to experience the same outcomes when pointed at Kafka-with-ACLs,
 * or proxy-with-authz (give or take some internal identifiers).
 */
class TopicTracingIT extends AbstractTracingIT {

    public static final String TOPIC_A = "topicA";
    public static final String TOPIC_B = "topicB";
    public static final String TXN_ID_P = "txnIdP";
    public static final String GROUP_1 = "group1";
    public static final String GROUP_2 = "group2";
    public static final IntegerSerializer INTEGER_SERIALIZER = new IntegerSerializer();
    public static final IntegerDeserializer INTEGER_DESERIALIZER = new IntegerDeserializer();

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    private Path rulesFile;

    private List<AclBinding> aclBindings;

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                allow User with name = "alice" to * Topic with name in {"%s", "%s"};
                allow User with name = "bob" to CREATE Topic with name = "%s";
                otherwise deny;
                """.formatted(TOPIC_A, TOPIC_B, TOPIC_A));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_A, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_B, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TXN_ID_P, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_1, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_2, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_A, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.CREATE, AclPermissionType.ALLOW)));
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

    record AdminProg() implements Prog {

        @Override
        public void run(ClientFactory clientFactory) {
            String topicA = TOPIC_A;
            String topicB = TOPIC_B;

            String setupUser = ALICE;
            String adminUser = ALICE;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"))) {
                setup.createTopic(topicA);
                Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(() -> ClusterPrepUtils.allTopicPartitionsHaveALeader(setup.admin(), List.of(TOPIC_A)));
                try (var admin = clientFactory.newAdmin(adminUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "admin"))) {
                    admin.createPartitions(topicA, 2);
                    Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(() -> ClusterPrepUtils.allTopicPartitionsHaveALeader(admin.admin(), List.of(TOPIC_A)));
                    admin.describeTopic(topicA);
                    admin.describeConfigs(ConfigResource.Type.TOPIC, topicA);
                    admin.alterConfigs(ConfigResource.Type.TOPIC, topicA, new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd"));
                    var topicBId = setup.createTopic(topicB).value();

                    admin.deleteTopic(topicBId);
                }
                setup.deleteTopic(topicA);
            }
        }
    }

    record SimpleProg(int numSends) implements Prog {

        @Override
        public void run(ClientFactory clientFactory) {
            String topicA = TOPIC_A;
            TopicPartition topicAPartition0 = new TopicPartition(topicA, 0);
            TopicPartition topicAPartition1 = new TopicPartition(topicA, 1);

            String setupUser = ALICE;
            String producerUser = ALICE;
            String consumerUser = ALICE;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"))) {
                setup.createTopic(topicA);
                try (var nonTransactionalProducer = clientFactory.newProducer(producerUser, INTEGER_SERIALIZER, Map.of(
                        ProducerConfig.CLIENT_ID_CONFIG, "nonTransactionalProducer",
                        ProducerConfig.PARTITIONER_CLASS_CONFIG, AlwaysPartition0Partitioner.class.getName()));
                        var nonGroupedConsumer = clientFactory.newConsumer(consumerUser, INTEGER_DESERIALIZER, Map.of(
                                ProducerConfig.CLIENT_ID_CONFIG, "nonGroupedConsumer",
                                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                                ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1))) {
                    for (int i = 0; i < numSends; i++) {
                        nonTransactionalProducer.send(topicA, i);
                    }

                    nonGroupedConsumer.assign(List.of(topicAPartition0, topicAPartition1));
                    // poll(),
                    nonGroupedConsumer.seekToBeginning(List.of(topicAPartition0, topicAPartition1));
                    nonGroupedConsumer.poll();
                    nonGroupedConsumer.commitSync();
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
            TopicPartition topicAPartition1 = new TopicPartition(topicA, 1);

            String setupUser = ALICE;
            String producerUser = ALICE;
            String consumerUser = ALICE;
            String adminUser = ALICE;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"))) {
                setup.createTopic(topicA);
                try (var transactionalProducer = clientFactory.newProducer(producerUser, INTEGER_SERIALIZER, Map.of(
                        ProducerConfig.CLIENT_ID_CONFIG, "transactionalProducer",
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG, TXN_ID_P,
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

                    try (var consumer = clientFactory.newConsumer(consumerUser, INTEGER_DESERIALIZER, Map.of(
                            ConsumerConfig.CLIENT_ID_CONFIG, "consumer",
                            ConsumerConfig.GROUP_ID_CONFIG, GROUP_1,
                            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, GROUP_1 + "-instance-1",
                            ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1))) {
                        consumer.subscribe(TOPIC_A);
                        consumer.poll();
                        consumer.seekToBeginning(List.of(topicAPartition0, topicAPartition1));
                        consumer.poll();
                        consumer.commitSync();
                    }

                    // Commit
                    transactionalProducer.beginTransaction().value();
                    var sent = transactionalProducer.send(topicA, 4).value().offset();
                    transactionalProducer.commitTransaction().value();

                    try (var consumer2 = clientFactory.newConsumer(consumerUser, INTEGER_DESERIALIZER, Map.of(
                            ConsumerConfig.CLIENT_ID_CONFIG, "consumer2",
                            ConsumerConfig.GROUP_ID_CONFIG, GROUP_2,
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, GROUP_2 + "-instance-1",
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

                    try (var admin = clientFactory.newAdmin(adminUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "admin"))) {
                        ConsumerGroupDescription actual = admin.describeConsumerGroup(GROUP_2).value();
                        assertThat(actual.groupId()).isEqualTo(GROUP_2);
                        assertThat(actual.members()).hasSize(1);
                    }
                }
                setup.deleteTopic(topicA);
            }
        }
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        var alice = new Actor(ALICE, "Alice", Map.of(), Map.of(), Map.of());
        var eve = new Actor(EVE, "Eve", Map.of(), Map.of(), Map.of());
        // TODO support new consumer protocol
        // Arguments newConsumerProtocol = Arguments.of(new TransactionalProg(), List.of(alice.withConsumerConfigOverrides(Map.of(
        // ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT))),
        // eve.withConsumerConfigOverrides(Map.of(
        // ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)))));
        return List.of(
                Arguments.of(new AdminProg(), List.of(alice, eve)),
                Arguments.of(new SimpleProg(1), List.of(alice, eve)),
                Arguments.of(new SimpleProg(1), List.of(
                        alice.withProducerConfigOverrides(ProducerConfig.ACKS_CONFIG, "0"),
                        eve.withProducerConfigOverrides(ProducerConfig.ACKS_CONFIG, "0"))),
                Arguments.of(new SimpleProg(100), List.of(
                        alice.withProducerConfigOverrides(Map.of(
                                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 100,
                                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false)),
                        eve.withProducerConfigOverrides(Map.of(
                                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 100,
                                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false)))),
                Arguments.of(new TransactionalProg(), List.of(
                        alice.withConsumerConfigOverrides(Map.of(
                                ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
                                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000)),
                        eve.withConsumerConfigOverrides(Map.of(
                                ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
                                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000)))));
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(Prog prog, List<Actor> actors) throws Exception {
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
