/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.filter.entityisolation;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConfigResourcesOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.ShareAcknowledgementMode;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.filter.entityisolation.EntityIsolation;
import io.kroxylicious.filter.entityisolation.PrincipalEntityNameMapperService;
import io.kroxylicious.filter.sasl.inspection.SaslInspection;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Name;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.collection;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for Entity Isolation Filter.
 * <br/>
 * These tests use the Kafka Client API. They aim to show that entity isolation
 * is functioning correctly. Owing to the nature of the API, these tests exercise
 * several RPCs at once.
 */
@ExtendWith(KafkaClusterExtension.class)
class EntityIsolationIT {

    private static final String CONSUMER_GROUP_NAME = "mygroup";

    @SaslMechanism(principals = { @SaslMechanism.Principal(user = "alice", password = "pwd"), @SaslMechanism.Principal(user = "bob", password = "pwd"),
            @SaslMechanism.Principal(user = "dash-boy", password = "pwd") })
    KafkaCluster cluster;

    private enum ConsumerStyle {
        ASSIGN,
        SUBSCRIBE
    }

    /**
     * Group isolation - describe groups.
     * <br/>
     * Alice and Bob both consume from the same topic using distinct own group names.
     * Test uses describeConsumerGroups to ensure that Alice only sees her group and Bob only sees his.
     *
     * @param consumerStyle consumer style
     * @param topic topic
     */
    @ParameterizedTest
    @EnumSource(value = ConsumerStyle.class)
    void describeGroupMaintainsGroupIsolation(ConsumerStyle consumerStyle, Topic topic) {
        ensureGroupIsolationMaintained(consumerStyle, cluster, topic, true);
    }

    /**
     * Share Group isolation - describe groups.
     * <br/>
     * Alice and Bob both consume from the same topic using distinct own group names.
     * Test uses describeShareConsumerGroups to ensure that Alice only sees her group and Bob only sees his.
     *
     * @param topic topic
     */
    @Test
    void describeShareGroupMaintainsGroupIsolation(Topic topic) {
        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.GROUP_ID));

        var aliceConfig = buildClientConfig("alice", "pwd");
        var bobConfig = buildClientConfig("bob", "pwd");
        try (var tester = kroxyliciousTester(configBuilder);
                var aliceAdmin = tester.admin(aliceConfig);
                var bobAdmin = tester.admin(bobConfig)) {

            produceRecords(topic, tester, aliceConfig, 1);

            String aliceGroup = "AliceGroup";
            String bobGroup = "BobGroup";
            try {
                prepareClusterForShareGroups(aliceAdmin, aliceGroup);
                prepareClusterForShareGroups(bobAdmin, bobGroup);

                runShareConsumerInOrderToCreateGroup(tester, aliceGroup, topic, aliceConfig, 1);
                runShareConsumerInOrderToCreateGroup(tester, bobGroup, topic, bobConfig, 1);

                verifyShareConsumerGroupsWithDescribe(aliceAdmin, Set.of(aliceGroup), Set.of(bobGroup, "idontexist"));
                verifyShareConsumerGroupsWithDescribe(bobAdmin, Set.of(bobGroup), Set.of(aliceGroup, "idontexist"));
            }
            finally {
                assertThat(aliceAdmin.deleteShareGroups(Set.of(aliceGroup)).all()).succeedsWithin(Duration.ofSeconds(5));
                assertThat(bobAdmin.deleteShareGroups(Set.of(bobGroup)).all()).succeedsWithin(Duration.ofSeconds(5));
            }
        }
    }

    /**
     * Group isolation - list groups.
     * <br/>
     * Alice and Bob both consumer from the same topic using distinct own group names.
     * Test uses listGroups to ensure that Alice only sees her group and Bob only sees his.
     *
     * @param consumerStyle consumer style
     * @param topic topic
     */
    @ParameterizedTest
    @EnumSource(value = ConsumerStyle.class)
    void listGroupMaintainsGroupIsolation(ConsumerStyle consumerStyle,
                                          Topic topic) {
        ensureGroupIsolationMaintained(consumerStyle, cluster, topic, false);
    }

    private void ensureGroupIsolationMaintained(ConsumerStyle consumerStyle, KafkaCluster cluster, Topic topic, boolean useDescribe) {
        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.GROUP_ID));

        var aliceConfig = buildClientConfig("alice", "pwd");
        var bobConfig = buildClientConfig("bob", "pwd");
        try (var tester = kroxyliciousTester(configBuilder);
                var aliceAdmin = tester.admin(aliceConfig);
                var bobAdmin = tester.admin(bobConfig)) {
            runConsumerInOrderToCreateGroup(tester, "AliceGroup", topic, consumerStyle, aliceConfig);
            runConsumerInOrderToCreateGroup(tester, "BobGroup", topic, consumerStyle, bobConfig);

            if (useDescribe) {
                verifyConsumerGroupsWithDescribe(aliceAdmin, Set.of("AliceGroup"), Set.of("BobGroup", "idontexist"));
                verifyConsumerGroupsWithDescribe(bobAdmin, Set.of("BobGroup"), Set.of("AliceGroup", "idontexist"));
            }
            else {
                verifyConsumerGroupsWithList(aliceAdmin, Set.of("AliceGroup"));
                verifyConsumerGroupsWithList(bobAdmin, Set.of("BobGroup"));
            }
        }
    }

    /**
     * Group isolation - consumers and offsets.
     * <br/>
     * Alice and Bob both using the same group name, but entity isolation ensures that the two
     * groups are actually independent.  This test ensures that the offsets for the two consumer
     * groups are indeed maintained independently.
     *
     * @param topic topic
     */
    @Test
    void consumerGroupOffsetCommitMaintainsGroupIsolation(Topic topic) {

        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.GROUP_ID));
        var aliceConfig = buildClientConfig("alice", "pwd",
                Map.of(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, UUID.randomUUID().toString()));
        var bobConfig = buildClientConfig("bob", "pwd",
                Map.of(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, UUID.randomUUID().toString()));

        try (var tester = kroxyliciousTester(configBuilder)) {

            try (var producer = tester.producer(aliceConfig)) {
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "k1", "v1")))
                        .succeedsWithin(Duration.ofSeconds(5));
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "k2", "v2")))
                        .succeedsWithin(Duration.ofSeconds(5));
            }

            try (var aliceConsumer = tester.consumer(aliceConfig)) {
                aliceConsumer.subscribe(List.of(topic.name()));
                var aliceRecs = aliceConsumer.poll(Duration.ofSeconds(5));

                assertThat(aliceRecs).hasSize(2);

                var first = aliceRecs.records(topic.name()).iterator().next();
                // commit the first record leaving the second one to consume later
                aliceConsumer.commitSync(Map.of(new TopicPartition(topic.name(), first.partition()), new OffsetAndMetadata(first.offset() + 1)));
            }

            try (var bobConsumer = tester.consumer(bobConfig)) {
                bobConsumer.subscribe(List.of(topic.name()));
                var bobRecs = bobConsumer.poll(Duration.ofSeconds(5));

                assertThat(bobRecs)
                        .withFailMessage("Bob's group should be independent of Alice's. He should be able to consume two records")
                        .hasSize(2);
            }

            try (var aliceRound2Consumer = tester.consumer(aliceConfig)) {
                aliceRound2Consumer.subscribe(List.of(topic.name()));
                var aliceRecs = aliceRound2Consumer.poll(Duration.ofSeconds(10));

                assertThat(aliceRecs)
                        .singleElement()
                        .withFailMessage("Alice should be able to restart consuming from where she left off")
                        .extracting(ConsumerRecord::key)
                        .isEqualTo("k2");
            }
        }
    }

    /**
     * Group isolation - list offsets.
     * <br/>
     * Ensure the offsets committed to an isolated group are visible through the {@link Admin#listConsumerGroupOffsets(String)} API.
     *
     * @param topic topic
     */
    @Test
    void listConsumerGroupOffsets(Topic topic) {

        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.GROUP_ID));
        var aliceConfig = buildClientConfig("alice", "pwd",
                Map.of(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, UUID.randomUUID().toString()));

        try (var tester = kroxyliciousTester(configBuilder)) {

            try (var producer = tester.producer(aliceConfig)) {
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "k1", "v1")))
                        .succeedsWithin(Duration.ofSeconds(5));
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "k2", "v2")))
                        .succeedsWithin(Duration.ofSeconds(5));
            }

            try (var aliceConsumer = tester.consumer(aliceConfig)) {
                aliceConsumer.subscribe(List.of(topic.name()));
                var aliceRecs = aliceConsumer.poll(Duration.ofSeconds(5));

                assertThat(aliceRecs).hasSize(2);

                var first = aliceRecs.records(topic.name()).iterator().next();
                // commit the first record leaving the second one to consume later
                aliceConsumer.commitSync(Map.of(new TopicPartition(topic.name(), first.partition()), new OffsetAndMetadata(first.offset() + 1)));
            }

            try (var aliceAdmin = tester.admin(aliceConfig)) {
                var actual = assertThat(aliceAdmin.listConsumerGroupOffsets(CONSUMER_GROUP_NAME).all())
                        .succeedsWithin(Duration.ofSeconds(30))
                        .actual();

                assertThat(actual)
                        .extractingByKey(CONSUMER_GROUP_NAME, as(map(TopicPartition.class, OffsetAndMetadata.class)))
                        .extractingByKey(new TopicPartition(topic.name(), 0))
                        .satisfies(oam -> assertThat(oam.offset()).isEqualTo(1));
            }
        }
    }

    /**
     * Group isolation - delete group
     * <br/>
     * Verifies that an isolated group can be deleted.
     *
     * @param topic topic
     */
    @Test
    void deleteConsumerGroups(Topic topic) {
        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.GROUP_ID));

        var aliceConfig = buildClientConfig("alice", "pwd");
        try (var tester = kroxyliciousTester(configBuilder);
                var aliceAdmin = tester.admin(aliceConfig)) {
            runConsumerInOrderToCreateGroup(tester, "AliceGroup1", topic, ConsumerStyle.ASSIGN, aliceConfig);
            runConsumerInOrderToCreateGroup(tester, "AliceGroup2", topic, ConsumerStyle.ASSIGN, aliceConfig);

            verifyConsumerGroupsWithList(aliceAdmin, Set.of("AliceGroup1", "AliceGroup2"));

            assertThat(aliceAdmin.deleteConsumerGroups(List.of("AliceGroup1")).all())
                    .succeedsWithin(Duration.ofSeconds(10));

            verifyConsumerGroupsWithList(aliceAdmin, Set.of("AliceGroup2"));
        }
    }

    /**
     * Share group isolation - list offsets.
     * <br/>
     * Ensure the offsets acknowledge to a share group are visible through the {@link Admin#listConsumerGroupOffsets(String)} API.
     *
     * @param topic topic
     */
    @Test
    void listShareConsumerGroupOffsets(Topic topic) {

        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.GROUP_ID));
        var aliceConfig = buildClientConfig("alice", "pwd",
                Map.of(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_NAME,
                        ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, ShareAcknowledgementMode.EXPLICIT.name()));

        try (var tester = kroxyliciousTester(configBuilder);
                var aliceAdmin = tester.admin(aliceConfig)) {

            prepareClusterForShareGroups(aliceAdmin, CONSUMER_GROUP_NAME);

            try (var producer = tester.producer(aliceConfig)) {
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "k1", "v1")))
                        .succeedsWithin(Duration.ofSeconds(5));
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "k2", "v2")))
                        .succeedsWithin(Duration.ofSeconds(5));
            }

            try (var aliceConsumer = tester.shareConsumer(aliceConfig)) {
                aliceConsumer.subscribe(List.of(topic.name()));
                var aliceRecs = aliceConsumer.poll(Duration.ofSeconds(10));

                assertThat(aliceRecs).hasSize(2);

                var first = aliceRecs.records(topic.name()).iterator().next();
                // commit the first record leaving the second one to consume later
                aliceConsumer.acknowledge(first);
            }

            var spec = new ListShareGroupOffsetsSpec().topicPartitions(List.of(new TopicPartition(topic.name(), 0)));
            var actual = assertThat(aliceAdmin.listShareGroupOffsets(Map.of(CONSUMER_GROUP_NAME, spec)).all())
                    .succeedsWithin(Duration.ofSeconds(30))
                    .actual();

            assertThat(actual)
                    .extractingByKey(CONSUMER_GROUP_NAME, as(map(TopicPartition.class, SharePartitionOffsetInfo.class)))
                    .extractingByKey(new TopicPartition(topic.name(), 0))
                    .satisfies(spoi -> assertThat(spoi.startOffset()).isEqualTo(1));

            assertThat(aliceAdmin.deleteShareGroups(Set.of(CONSUMER_GROUP_NAME)).all()).succeedsWithin(Duration.ofSeconds(5));
        }
    }

    /**
     * Group isolation - delete share group
     * <br/>
     * Verifies that an isolated share group can be deleted.
     *
     * @param topic topic
     */
    @Test
    void deleteShareConsumerGroups(Topic topic) {
        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.GROUP_ID));

        var aliceConfig = buildClientConfig("alice", "pwd");
        try (var tester = kroxyliciousTester(configBuilder);
                var aliceAdmin = tester.admin(aliceConfig)) {

            produceRecords(topic, tester, aliceConfig, 1);

            var aliceGroup1 = "AliceGroup1";
            var aliceGroup2 = "AliceGroup2";
            prepareClusterForShareGroups(aliceAdmin, aliceGroup1);
            prepareClusterForShareGroups(aliceAdmin, aliceGroup2);

            runShareConsumerInOrderToCreateGroup(tester, aliceGroup1, topic, aliceConfig, 1);
            runShareConsumerInOrderToCreateGroup(tester, aliceGroup2, topic, aliceConfig, 1);

            verifyShareConsumerGroupsWithDescribe(aliceAdmin, Set.of(aliceGroup1, aliceGroup2), Set.of());

            assertThat(aliceAdmin.deleteShareGroups(List.of(aliceGroup1)).all())
                    .succeedsWithin(Duration.ofSeconds(10));

            verifyShareConsumerGroupsWithDescribe(aliceAdmin, Set.of(aliceGroup2), Set.of(aliceGroup1));

            assertThat(aliceAdmin.deleteShareGroups(List.of(aliceGroup2)).all())
                    .succeedsWithin(Duration.ofSeconds(10));
        }
    }

    /**
     * Group isolation - config
     * <br/>
     * Verifies that a group config is properly isolated.
     * Bob should not be able to see alice's group.
     */
    @Test
    void groupConfigIsolation() {
        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.GROUP_ID));

        var aliceConfig = buildClientConfig("alice", "pwd");
        var bobConfig = buildClientConfig("bob", "pwd");

        try (var tester = kroxyliciousTester(configBuilder)) {
            try (var aliceAdmin = tester.admin(aliceConfig)) {
                var configResource = new ConfigResource(ConfigResource.Type.GROUP, "aliceconfig");
                var autoOffsetReset = new ConfigEntry(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "earliest");

                var setConfig = List.of(new AlterConfigOp(autoOffsetReset, AlterConfigOp.OpType.SET));
                assertThat(aliceAdmin.incrementalAlterConfigs(Map.of(configResource, setConfig)).all())
                        .succeedsWithin(Duration.ofSeconds(5));
            }

            try (var bobAdmin = tester.admin(bobConfig)) {
                var configResource = new ConfigResource(ConfigResource.Type.GROUP, "bobconfig");
                var autoOffsetReset = new ConfigEntry(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "earliest");

                var setConfig = List.of(new AlterConfigOp(autoOffsetReset, AlterConfigOp.OpType.SET));
                assertThat(bobAdmin.incrementalAlterConfigs(Map.of(configResource, setConfig)).all())
                        .succeedsWithin(Duration.ofSeconds(5));

                assertThat(bobAdmin.listConfigResources(Set.of(ConfigResource.Type.GROUP), new ListConfigResourcesOptions()).all())
                        .succeedsWithin(Duration.ofSeconds(5))
                        .asInstanceOf(InstanceOfAssertFactories.list(ConfigResource.class))
                        .containsExactly(configResource);
            }
        }
    }

    /**
     * Verifies that a transaction used on the produce side is functional when isolated.
     *
     * @param topic topic
     */
    @Test
    void produceInTransaction(Topic topic) {
        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.TRANSACTIONAL_ID));

        var transactionalId = "mytxn";
        var aliceConfig = buildClientConfig("alice", "pwd",
                Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId));

        try (var tester = kroxyliciousTester(configBuilder)) {
            try (var admin = tester.admin(aliceConfig)) {
                produceInTransaction(topic, tester, aliceConfig, true);

                assertThat(admin.listTransactions(new ListTransactionsOptions()).all())
                        .succeedsWithin(Duration.ofSeconds(5))
                        .satisfies(tl -> {
                            assertThat(tl.iterator())
                                    .toIterable()
                                    .singleElement()
                                    .satisfies(t -> {
                                        assertThat(t.transactionalId()).isEqualTo(transactionalId);
                                        assertThat(t.state()).isEqualTo(TransactionState.COMPLETE_COMMIT);
                                    });
                        });
            }
        }
    }

    /**
     * Verifies that a transaction used on the produce and consumer with offset committing is functional when isolated.
     * <br/>
     * Note: KIP-932 (Share Groups) didn't include support for enlisting share group acknowledgement into transactions
     * so we don't need to test transactions with share groups.
     *
     * @param input topic
     * @param output topic
     */
    @Test
    void produceAndConsumeInTransaction(Topic input, Topic output) {
        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.TRANSACTIONAL_ID));

        var transactionalId = "mytxn";
        var aliceConfig = buildClientConfig("alice", "pwd");

        try (var tester = kroxyliciousTester(configBuilder)) {
            // put some records in an input topic
            produceRecords(input, tester, aliceConfig, 2);

            var aliceConfigForTxnProduce = new HashMap<>(aliceConfig);
            aliceConfigForTxnProduce.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

            var aliceConfigForConsumer = new HashMap<>(aliceConfig);
            aliceConfigForConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            aliceConfigForConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, "AliceGroup");

            // now consume and from input and produce to output, using a transaction.
            try (var producer = tester.producer(aliceConfigForTxnProduce);
                    var consumer = tester.consumer(aliceConfigForConsumer)) {
                producer.initTransactions();

                consumer.subscribe(List.of(input.name()));

                int processed = 0;
                do {
                    var records = consumer.poll(Duration.ofSeconds(30));
                    for (var r : records) {
                        producer.beginTransaction();

                        var key = r.key();
                        var value = r.value();
                        assertThat(producer.send(new ProducerRecord<>(output.name(), 0, key, value)))
                                .succeedsWithin(Duration.ofSeconds(5));

                        producer.sendOffsetsToTransaction(Map.of(new TopicPartition(r.topic(), r.partition()),
                                new OffsetAndMetadata(r.offset() + 1)), consumer.groupMetadata());
                        producer.commitTransaction();
                        processed++;
                    }
                } while (processed < 2);

                consumer.subscribe(List.of(output.name()));
                var records = consumer.poll(Duration.ofSeconds(30));
                assertThat(records.iterator())
                        .toIterable()
                        .hasSize(2)
                        .map(ConsumerRecord::value)
                        .containsExactly("v1", "v2");
            }
        }
    }

    /**
     * Verifies that transactions are isolated from each other when
     * Alice and Bob use the same transactionalId.
     *
     * @param topic topic
     */
    @Test
    void transactionIsolation(Topic topic) {
        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.TRANSACTIONAL_ID));

        var transactionalId = "mytxn";
        var aliceConfig = buildClientConfig("alice", "pwd",
                Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId));
        var bobConfig = buildClientConfig("bob", "pwd",
                Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId));

        try (var tester = kroxyliciousTester(configBuilder)) {
            produceInTransaction(topic, tester, aliceConfig, true);
            produceInTransaction(topic, tester, bobConfig, false);

            try (var aliceAdmin = tester.admin(aliceConfig)) {
                assertThat(aliceAdmin.listTransactions(new ListTransactionsOptions()).all())
                        .succeedsWithin(Duration.ofSeconds(5))
                        .satisfies(tl -> {
                            assertThat(tl.iterator())
                                    .toIterable()
                                    .singleElement()
                                    .satisfies(t -> {
                                        assertThat(t.transactionalId()).isEqualTo(transactionalId);
                                        assertThat(t.state()).isEqualTo(TransactionState.COMPLETE_COMMIT);
                                    });
                        });
            }

            try (var bobAdmin = tester.admin(bobConfig)) {
                assertThat(bobAdmin.listTransactions(new ListTransactionsOptions()).all())
                        .succeedsWithin(Duration.ofSeconds(5))
                        .satisfies(tl -> {
                            assertThat(tl.iterator())
                                    .toIterable()
                                    .singleElement()
                                    .satisfies(t -> {
                                        assertThat(t.transactionalId()).isEqualTo(transactionalId);
                                        assertThat(t.state()).isEqualTo(TransactionState.COMPLETE_ABORT);
                                    });
                        });
            }
        }
    }

    /**
     * Verifies that transactions are filtered correctly (KIP-1152) when transaction isolation is used.
     * Alice uses three transactions 'cat', 'bat', 'dog' and lists them using the RE '^.at', expecting 'cat' and 'bat'
     * as the response.
     *
     * @param topic topic
     */
    @Test
    void transactionFilteringByClientRegex(Topic topic) {
        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.TRANSACTIONAL_ID));

        var cat = "cat";
        var bat = "bat";
        var dog = "dog";
        Function<Map<String, Object>, Map<String, Object>> configGenerator = addConfig -> buildClientConfig("alice", "pwd",
                addConfig);

        try (var tester = kroxyliciousTester(configBuilder)) {
            produceInTransaction(topic, tester, configGenerator.apply(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, cat)), true);
            produceInTransaction(topic, tester, configGenerator.apply(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, bat)), true);
            produceInTransaction(topic, tester, configGenerator.apply(Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, dog)), true);

            try (var aliceAdmin = tester.admin(configGenerator.apply(Map.of()))) {
                var options = new ListTransactionsOptions().filterOnTransactionalIdPattern("^.at");

                assertThat(aliceAdmin.listTransactions(options).all())
                        .succeedsWithin(Duration.ofSeconds(5))
                        .satisfies(tl -> {
                            assertThat(tl.iterator())
                                    .toIterable()
                                    .extracting(TransactionListing::transactionalId)
                                    .containsExactlyInAnyOrderElementsOf(List.of(cat, bat));
                        });
            }
        }
    }

    static Stream<Arguments> aclBindings() {
        return Stream.of(
                Arguments.argumentSet("group - literal",
                        new ResourcePattern(ResourceType.GROUP, "AliceGroup", PatternType.LITERAL),
                        new ResourcePattern(ResourceType.GROUP, "BobGroup", PatternType.LITERAL),
                        List.of(EntityIsolation.EntityType.GROUP_ID)),
                Arguments.argumentSet("group - prefixed",
                        new ResourcePattern(ResourceType.GROUP, "AliceGroup", PatternType.PREFIXED),
                        new ResourcePattern(ResourceType.GROUP, "BobGroup", PatternType.PREFIXED),
                        List.of(EntityIsolation.EntityType.GROUP_ID)),
                Arguments.argumentSet("transactionalId - literal",
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "AliceTxn", PatternType.LITERAL),
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "BobTxn", PatternType.PREFIXED),
                        List.of(EntityIsolation.EntityType.TRANSACTIONAL_ID)));
    }

    /**
     * Verifies that ACL bindings are properly isolated.
     * Alice and Bob both create an ACL binding.  The test ensure that neither Alice nor Bob
     * can see each other's bindings.
     *
     * @param authzCluster cluster with authorization
     */
    @ParameterizedTest
    @MethodSource("aclBindings")
    void aclRuleWithResourceTypeIsolation(ResourcePattern aliceResourcePattern,
                                          ResourcePattern bobResourcePattern,
                                          List<EntityIsolation.EntityType> mappedResourceTypes,
                                          @SaslMechanism(principals = { @SaslMechanism.Principal(user = "alice", password = "pwd"),
                                                  @SaslMechanism.Principal(user = "bob", password = "pwd") }) @BrokerConfig(name = "authorizer.class.name", value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer") @BrokerConfig(name = "super.users", value = "User:ANONYMOUS;User:alice;User:bob") @Name("authz") KafkaCluster authzCluster) {
        var configBuilder = buildProxyConfig(authzCluster, mappedResourceTypes);

        var aliceConfig = buildClientConfig("alice", "pwd");
        var bobConfig = buildClientConfig("bob", "pwd");

        try (var tester = kroxyliciousTester(configBuilder)) {
            try (var aliceAdmin = tester.admin(aliceConfig)) {
                var aclBinding = new AclBinding(aliceResourcePattern,
                        new AccessControlEntry("User:foo", "*", AclOperation.ALL, AclPermissionType.ALLOW));
                assertThat(aliceAdmin.createAcls(List.of(aclBinding)).all())
                        .succeedsWithin(Duration.ofSeconds(5));
            }
            try (var bobAdmin = tester.admin(bobConfig)) {
                var aclBinding = new AclBinding(bobResourcePattern,
                        new AccessControlEntry("User:bar", "*", AclOperation.ALL, AclPermissionType.ALLOW));
                assertThat(bobAdmin.createAcls(List.of(aclBinding)).all())
                        .succeedsWithin(Duration.ofSeconds(5));

                assertThat(bobAdmin.describeAcls(new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
                        AccessControlEntryFilter.ANY)).values())
                        .succeedsWithin(Duration.ofSeconds(5))
                        .asInstanceOf(collection(AclBinding.class))
                        .extracting(AclBinding::pattern)
                        .extracting(ResourcePattern::name)
                        .containsExactlyInAnyOrderElementsOf(List.of(bobResourcePattern.name()));
            }
        }
    }

    /**
     * Rejects unacceptable principal
     * @param topic topic
     */
    @Test
    void rejectsUnacceptablePrincipal(Topic topic) {

        var configBuilder = buildProxyConfig(cluster, List.of(EntityIsolation.EntityType.GROUP_ID));
        // The principal will be rejected as the dash is also the separator used by the mapper.
        var dashboy = buildClientConfig("dash-boy", "pwd");

        try (var tester = kroxyliciousTester(configBuilder)) {

            try (var consumer = tester.consumer(dashboy)) {

                assertThatThrownBy(() -> runConsumerInOrderToCreateGroup(tester, "DashBoy", topic, ConsumerStyle.ASSIGN, dashboy))
                        .isInstanceOf(UnknownServerException.class);
            }
        }
    }

    private static void produceInTransaction(Topic topic, KroxyliciousTester tester, Map<String, Object> aliceConfig, boolean commit) {
        try (var producer = tester.producer(aliceConfig)) {
            producer.initTransactions();
            producer.beginTransaction();

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k1", "v1")))
                    .succeedsWithin(Duration.ofSeconds(5));

            if (commit) {
                producer.commitTransaction();
            }
            else {
                producer.abortTransaction();
            }
        }
    }

    private static ConfigurationBuilder buildProxyConfig(KafkaCluster cluster, List<EntityIsolation.EntityType> entityTypes) {
        var configBuilder = KroxyliciousConfigUtils.proxy(cluster);

        var saslInspectionFilter = new NamedFilterDefinitionBuilder(
                SaslInspection.class.getName(),
                SaslInspection.class.getName());

        saslInspectionFilter.withConfig("enabledMechanisms", Set.of("PLAIN"));
        var saslInspection = saslInspectionFilter.build();

        var entityIsolationFilter = new NamedFilterDefinitionBuilder(
                EntityIsolation.class.getName(),
                EntityIsolation.class.getName());

        entityIsolationFilter.withConfig("entityTypes", entityTypes,
                "mapper", PrincipalEntityNameMapperService.class.getSimpleName(),
                "mapperConfig", Map.of("principalType", User.class.getName()));
        var entityIsolation = entityIsolationFilter.build();

        configBuilder.addToFilterDefinitions(saslInspection, entityIsolation)
                .addToDefaultFilters(saslInspection.name(), entityIsolation.name());
        return configBuilder;
    }

    private void verifyConsumerGroupsWithDescribe(Admin admin, Set<String> expectedPresent, Set<String> expectedAbsent) {
        assertThat(admin.describeConsumerGroups(expectedPresent).all())
                .succeedsWithin(Duration.ofSeconds(5))
                .asInstanceOf(map(String.class, ConsumerGroupDescription.class))
                .allSatisfy((s, consumerGroupDescription) -> assertThat(consumerGroupDescription.groupState()).isNotIn(GroupState.DEAD));

        expectedAbsent.forEach(absent -> {
            var set = Set.of(absent);
            assertThat(admin.describeConsumerGroups(set).all())
                    .withFailMessage("Expected group %s to be reported absent but was not", absent)
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .havingRootCause()
                    .isInstanceOf(GroupIdNotFoundException.class);
        });
    }

    private void verifyShareConsumerGroupsWithDescribe(Admin admin, Set<String> expectedPresent, Set<String> expectedAbsent) {
        assertThat(admin.describeShareGroups(expectedPresent).all())
                .succeedsWithin(Duration.ofSeconds(5))
                .asInstanceOf(map(String.class, ShareGroupDescription.class))
                .allSatisfy((s, shareGroupDescription) -> assertThat(shareGroupDescription.groupState()).isNotIn(GroupState.DEAD));

        expectedAbsent.forEach(absent -> {
            var set = Set.of(absent);
            assertThat(admin.describeShareGroups(set).all())
                    .withFailMessage("Expected group %s to be reported absent but was not", absent)
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .havingRootCause()
                    .isInstanceOf(GroupIdNotFoundException.class);
        });
    }

    private void verifyConsumerGroupsWithList(Admin admin, Set<String> expected) {
        assertThat(admin.listGroups().all())
                .succeedsWithin(Duration.ofSeconds(5))
                .asInstanceOf(collection(GroupListing.class))
                .extracting(GroupListing::groupId)
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    private static Map<String, Object> buildClientConfig(String username, String password) {
        return buildClientConfig(username, password, Map.of());
    }

    private static Map<String, Object> buildClientConfig(String username, String password, Map<String, Object> additionalConfig) {
        var config = new HashMap<>(additionalConfig);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        config.put(SaslConfigs.SASL_JAAS_CONFIG,
                String.format("""
                        %s required username="%s" password="%s";""",
                        PlainLoginModule.class.getName(), username, password));
        config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        return config;
    }

    private void runConsumerInOrderToCreateGroup(KroxyliciousTester tester, String groupId, Topic topic, ConsumerStyle consumerStyle,
                                                 Map<String, Object> clientConfig) {
        var consumerConfig = new HashMap<>(clientConfig);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        try (var consumer = tester.consumer(consumerConfig)) {

            if (consumerStyle == ConsumerStyle.ASSIGN) {
                consumer.assign(List.of(new TopicPartition(topic.name(), 0)));
            }
            else {
                var listener = new PartitionAssignmentAwaitingRebalanceListener<>(consumer);
                consumer.subscribe(List.of(topic.name()), listener);
                listener.awaitAssignment(Duration.ofMinutes(1));
            }

            var zeroOffset = new OffsetAndMetadata(0);
            consumer.commitSync(consumer.assignment().stream().collect(Collectors.toMap(Function.identity(), a -> zeroOffset)));
        }
    }

    private void runShareConsumerInOrderToCreateGroup(KroxyliciousTester tester,
                                                      String groupId,
                                                      Topic topic,
                                                      Map<String, Object> saslConfig,
                                                      int numberOfRecords) {
        var consumerConfig = new HashMap<>(saslConfig);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfig.putAll(tester.clientConfiguration());

        try (var shareConsumer = tester.shareConsumer(consumerConfig)) {
            shareConsumer.subscribe(List.of(topic.name()));
            var recs = shareConsumer.poll(Duration.ofSeconds(10));
            assertThat(recs).hasSize(numberOfRecords);
        }
    }

    private static void prepareClusterForShareGroups(Admin admin, String groupName) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, groupName);
        ConfigEntry autoOffsetReset = new ConfigEntry(GroupConfig.SHARE_AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<AlterConfigOp> alterConfig = List.of(new AlterConfigOp(autoOffsetReset, AlterConfigOp.OpType.SET));
        assertThat(admin.incrementalAlterConfigs(Map.of(configResource, alterConfig)).all())
                .succeedsWithin(Duration.ofSeconds(5));
    }

    private static class PartitionAssignmentAwaitingRebalanceListener<K, V> implements ConsumerRebalanceListener {
        private final AtomicBoolean assigned = new AtomicBoolean();
        private final Consumer<K, V> consumer;

        PartitionAssignmentAwaitingRebalanceListener(Consumer<K, V> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // unused
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            assigned.set(true);
        }

        private void awaitAssignment(Duration timeout) {
            await().atMost(timeout).until(() -> {
                consumer.poll(Duration.ofMillis(50));
                return assigned.get();
            });
        }
    }

    private static void produceRecords(Topic topic, KroxyliciousTester tester, Map<String, Object> producerConfig, int numberOfRecords) {
        try (var producer = tester.producer(producerConfig)) {
            for (int i = 1; i <= numberOfRecords; i++) {
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "k" + i, "v" + i)))
                        .succeedsWithin(Duration.ofSeconds(5));
            }
        }
    }

}
