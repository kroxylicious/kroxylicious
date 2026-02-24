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
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.testing.kafka.junit5ext.Name;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

/**
 * A test that runs through a sequence of interactions using real high level Clients,
 * recording the outcomes or (expected) exceptions. This test isolates Group authorization
 * and does not aim to test authorization of other entities.
 * We expect the client to experience the same outcomes when pointed at Kafka-with-ACLs,
 * or proxy-with-authz (give or take some internal identifiers).
 */
class GroupTracingIT extends AbstractTracingIT {

    public static final String TOPIC = "topic";
    public static final String GROUP_1 = "group1";
    public static final String GROUP_2 = "group2";
    public static final String TXN_ID_PREFIX = "txnIdP";

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
                from io.kroxylicious.filter.authorization import GroupResource as Group;
                allow User with name = "alice" to * Group with name in {"%s", "%s"};
                allow User with name = "bob" to {DESCRIBE_CONFIGS, ALTER_CONFIGS, DESCRIBE, DELETE} Group with name = "%s";
                otherwise deny;
                """.formatted(GROUP_1, GROUP_2, GROUP_1));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_1, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_2, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_1, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE_CONFIGS, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_1, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALTER_CONFIGS, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_1, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DELETE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_1, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC, PatternType.LITERAL),
                        new AccessControlEntry("User:" + EVE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TXN_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + EVE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TXN_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TXN_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(), aclBindings);
    }

    @AfterEach
    void tidyClusters() {
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(TOPIC), aclBindings);
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(TOPIC), List.of());
        ClusterPrepUtils.deleteAllConsumerGroups(kafkaClusterNoAuthzAdmin);
        ClusterPrepUtils.deleteAllConsumerGroups(kafkaClusterWithAuthzAdmin);
    }

    record GroupConsumerProg(String group, String consumerUser) implements Prog {

        @Override
        public void run(ClientFactory clientFactory) {

            String setupUser = ALICE;
            String producerUser = ALICE;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"));
                    var producer = clientFactory.newProducer(producerUser, new StringSerializer(), Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "producer"))) {
                setup.createTopic(TOPIC);
                Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(() -> ClusterPrepUtils.allTopicPartitionsHaveALeader(setup.admin(), List.of(TOPIC)));
                String recordValue = "arbitrary";
                Outcome<RecordMetadata> send = producer.send(TOPIC, recordValue);
                assertThat(send.isSuccess()).isTrue();
                try (var consumer = clientFactory.newConsumer(consumerUser, new StringDeserializer(),
                        groupConsumerConfig(group))) {
                    consumer.subscribe(TOPIC);
                    Outcome<ConsumerRecords<String, String>> poll = consumer.poll();
                    assertThat(poll.isSuccess()).isTrue();
                    assertThat(poll.value().count()).isEqualTo(1);
                    assertThat(poll.value().iterator().next()).satisfies(record -> {
                        assertThat(record.value()).isEqualTo(recordValue);
                    });
                    Outcome<Void> commitOutcome = consumer.commitSync();
                    assertThat(commitOutcome.isSuccess()).isTrue();
                }
                setup.deleteTopic(TOPIC);
            }
        }
    }

    record GroupAdminProg(String group, String adminUser) implements Prog {

        @Override
        public void run(ClientFactory clientFactory) {

            String setupUser = ALICE;
            String producerUser = ALICE;
            String consumerUser = ALICE;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"));
                    var producer = clientFactory.newProducer(producerUser, new StringSerializer(), Map.of(ProducerConfig.CLIENT_ID_CONFIG, "producer"));
                    var consumer = clientFactory.newConsumer(consumerUser, new StringDeserializer(), groupConsumerConfig(group))) {
                setup.createTopic(TOPIC);
                Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(() -> ClusterPrepUtils.allTopicPartitionsHaveALeader(setup.admin(), List.of(TOPIC)));
                String recordValue = "arbitrary";
                Outcome<RecordMetadata> send = producer.send(TOPIC, recordValue);
                assertThat(send.isSuccess()).isTrue();
                consumer.subscribe(TOPIC);
                Outcome<ConsumerRecords<String, String>> poll = consumer.poll();
                assertThat(poll.isSuccess()).isTrue();
                assertThat(poll.value().count()).isEqualTo(1);
                consumer.close();
                try (var admin = clientFactory.newAdmin(adminUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "admin"))) {
                    Outcome<ConsumerGroupDescription> descriptionOutcome = admin.describeConsumerGroup(group);
                    assertThat(descriptionOutcome.isSuccess()).isTrue();
                    assertThat(descriptionOutcome.value().groupState()).isEqualTo(GroupState.EMPTY);
                    Outcome<Void> adminAlter = admin.alterConfigs(ConfigResource.Type.GROUP, group, new ConfigEntry("consumer.session.timeout.ms", "58000"));
                    assertThat(adminAlter.isSuccess()).isTrue();
                    Outcome<Config> configOutcome = admin.describeConfigs(ConfigResource.Type.GROUP, group);
                    assertThat(configOutcome.isSuccess()).isTrue();
                    assertThat(configOutcome.value().get("consumer.session.timeout.ms")).satisfies(configEntry -> {
                        assertThat(configEntry.value()).isEqualTo("58000");
                    });
                    Outcome<Collection<GroupListing>> groupListings = admin.listGroups();
                    assertThat(groupListings.isSuccess()).isTrue();
                    assertThat(groupListings.value()).isNotEmpty().singleElement().satisfies(groupListing -> {
                        assertThat(groupListing.groupId()).isEqualTo(this.group);
                    });
                    Outcome<Void> deleteOutcome = admin.deleteGroups(List.of(group));
                    assertThat(deleteOutcome.isSuccess()).isTrue();
                }
            }
        }

    }

    record DeniedGroupAdminProg(String group, String adminUser) implements Prog {

        @Override
        public void run(ClientFactory clientFactory) {

            String setupUser = ALICE;
            String producerUser = ALICE;
            String consumerUser = ALICE;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"));
                    var producer = clientFactory.newProducer(producerUser, new StringSerializer(), Map.of(ProducerConfig.CLIENT_ID_CONFIG, "producer"));
                    var consumer = clientFactory.newConsumer(consumerUser, new StringDeserializer(), groupConsumerConfig(group))) {
                setup.createTopic(TOPIC);
                Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(() -> ClusterPrepUtils.allTopicPartitionsHaveALeader(setup.admin(), List.of(TOPIC)));
                String recordValue = "arbitrary";
                Outcome<RecordMetadata> send = producer.send(TOPIC, recordValue);
                assertThat(send.isSuccess()).isTrue();
                consumer.subscribe(TOPIC);
                Outcome<ConsumerRecords<String, String>> poll = consumer.poll();
                assertThat(poll.isSuccess()).isTrue();
                assertThat(poll.value().count()).isEqualTo(1);
                try (var admin = clientFactory.newAdmin(adminUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "admin"))) {
                    Outcome<ConsumerGroupDescription> descriptionOutcome = admin.describeConsumerGroup(group);
                    assertThat(descriptionOutcome.isSuccess()).isFalse();
                    Outcome<Void> adminAlter = admin.alterConfigs(ConfigResource.Type.GROUP, group, new ConfigEntry("consumer.session.timeout.ms", "58000"));
                    assertThat(adminAlter.isSuccess()).isFalse();
                    Outcome<Config> configOutcome = admin.describeConfigs(ConfigResource.Type.GROUP, group);
                    assertThat(configOutcome.isSuccess()).isFalse();
                    Outcome<Collection<GroupListing>> groupListings = admin.listGroups();
                    assertThat(groupListings.isSuccess());
                    assertThat(groupListings.value()).isEmpty();
                    Outcome<Void> deleteOutcome = admin.deleteGroups(List.of(group));
                    assertThat(deleteOutcome.isSuccess()).isFalse();
                }
            }
        }
    }

    @NonNull
    private static Map<String, Object> groupConsumerConfig(String group) {
        return Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest", ConsumerConfig.CLIENT_ID_CONFIG, " consumer",
                ConsumerConfig.GROUP_ID_CONFIG, group, ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1, ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 2000);
    }

    record DeniedGroupConsumerProg(String group, String consumerUser) implements Prog {

        @Override
        public void run(ClientFactory clientFactory) {

            String setupUser = ALICE;
            String producerUser = ALICE;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"));
                    var producer = clientFactory.newProducer(producerUser, new StringSerializer(), Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "producer"))) {
                setup.createTopic(TOPIC);
                Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(() -> ClusterPrepUtils.allTopicPartitionsHaveALeader(setup.admin(), List.of(TOPIC)));
                String recordValue = "arbitrary";
                Outcome<RecordMetadata> send = producer.send(TOPIC, recordValue);
                assertThat(send.isSuccess()).isTrue();
                try (var consumer = clientFactory.newConsumer(consumerUser, new StringDeserializer(),
                        groupConsumerConfig(group))) {
                    Outcome<Void> subscribe = consumer.subscribe(TOPIC);
                    assertThat(subscribe.isSuccess()).isTrue();
                    Outcome<ConsumerRecords<String, String>> poll = consumer.poll();
                    assertThat(poll.isSuccess()).isFalse();
                }
                setup.deleteTopic(TOPIC);
            }
        }
    }

    /**
     * Check that permitted producer can sendOffsetsToTransaction
     */
    static class TransactionalProg implements Prog {

        @Override
        @SuppressWarnings("java:S2925") // Thread.sleep
        public void run(ClientFactory clientFactory) throws InterruptedException {
            String topicA = TOPIC;
            TopicPartition topicAPartition0 = new TopicPartition(topicA, 0);

            String setupUser = ALICE;
            String producerUser = ALICE;
            String consumerUser = ALICE;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"))) {
                setup.createTopic(topicA);
                try (var transactionalProducer = clientFactory.newProducer(producerUser, new StringSerializer(), Map.of(
                        ProducerConfig.CLIENT_ID_CONFIG, "transactionalProducer",
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG, TXN_ID_PREFIX,
                        ProducerConfig.PARTITIONER_CLASS_CONFIG, TopicTracingIT.AlwaysPartition0Partitioner.class.getName()))) {

                    transactionalProducer.initTransactions();

                    // Commit
                    transactionalProducer.beginTransaction();
                    var sent = transactionalProducer.send(topicA, "arbitrary").value().offset();
                    transactionalProducer.commitTransaction();

                    try (var consumer = clientFactory.newConsumer(consumerUser, new StringDeserializer(), Map.of(
                            ConsumerConfig.CLIENT_ID_CONFIG, "consumer",
                            ConsumerConfig.GROUP_ID_CONFIG, GROUP_2,
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                            ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1))) {

                        consumer.subscribe(topicA);
                        ConsumerRecords<String, String> records;
                        Instant start = Instant.now();
                        while (true) {
                            if (Instant.now().minusSeconds(10).isAfter(start)) {
                                throw new RuntimeException("timed out waiting for records");
                            }
                            records = consumer.poll().value();
                            List<ConsumerRecord<String, String>> records1 = records.records(topicAPartition0);
                            if (!records1.isEmpty()
                                    && records1.get(records1.size() - 1).offset() >= sent) {
                                break;
                            }
                            Thread.sleep(100);
                        }

                        var offsets = records.nextOffsets();
                        assertThat(offsets).isNotEmpty();
                        assertThat(offsets.get(topicAPartition0).offset()).isGreaterThan(0);
                        var metadata = consumer.groupMetadata().value();
                        assertThat(metadata.generationId()).isGreaterThan(0);
                        assertThat(transactionalProducer.beginTransaction().isSuccess()).isTrue();
                        assertThat(transactionalProducer.sendOffsetsToTransaction(offsets, metadata).value()).isNull();
                        assertThat(transactionalProducer.commitTransaction().isSuccess()).isTrue();
                    }
                }
                setup.deleteTopic(topicA);
            }
        }
    }

    List<Arguments> shouldEnforceAccessToGroups() {
        var alice = new Actor(ALICE, PASSWORDS.get(ALICE), Map.of(), Map.of(), Map.of());
        var eve = new Actor(EVE, PASSWORDS.get(EVE), Map.of(), Map.of(), Map.of());
        var bob = new Actor(BOB, PASSWORDS.get(BOB), Map.of(), Map.of(), Map.of());
        return List.of(argumentSet("alice can consume from group " + GROUP_1, new GroupConsumerProg(GROUP_1, ALICE), List.of(alice)),
                argumentSet("alice can consume from group " + GROUP_2, new GroupConsumerProg(GROUP_2, ALICE), List.of(alice)),
                argumentSet("eve can not consume from group " + GROUP_1, new DeniedGroupConsumerProg(GROUP_1, EVE), List.of(alice, eve)),
                argumentSet("eve can not consume from group " + GROUP_2, new DeniedGroupConsumerProg(GROUP_1, EVE), List.of(alice, eve)),
                argumentSet("bob can not consume from group " + GROUP_1, new DeniedGroupConsumerProg(GROUP_1, BOB), List.of(alice, bob)),
                argumentSet("bob can not consume from group " + GROUP_2, new DeniedGroupConsumerProg(GROUP_1, BOB), List.of(alice, bob)),
                argumentSet("alice can administer group " + GROUP_1, new GroupAdminProg(GROUP_1, ALICE), List.of(alice)),
                argumentSet("alice can administer group " + GROUP_2, new GroupAdminProg(GROUP_2, ALICE), List.of(alice)),
                argumentSet("bob can administer group " + GROUP_1, new GroupAdminProg(GROUP_1, BOB), List.of(alice, bob)),
                argumentSet("bob can not administer group " + GROUP_2, new DeniedGroupAdminProg(GROUP_2, BOB), List.of(alice, bob)),
                argumentSet("eve can not administer group " + GROUP_1, new DeniedGroupAdminProg(GROUP_1, EVE), List.of(alice, eve)),
                argumentSet("eve can not administer group " + GROUP_2, new DeniedGroupAdminProg(GROUP_2, EVE), List.of(alice, eve)),
                argumentSet("alice can add offsets to transaction for group " + GROUP_1, new TransactionalProg(), List.of(alice)));
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToGroups(Prog prog, List<Actor> actors) throws Exception {
        assertProgTraceMatches(prog, actors, rulesFile);
    }

}
