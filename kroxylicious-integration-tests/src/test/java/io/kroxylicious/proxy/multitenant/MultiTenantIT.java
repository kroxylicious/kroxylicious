/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.multitenant;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicCollection.TopicNameCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.test.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.allOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@ExtendWith(KafkaClusterExtension.class)
class MultiTenantIT extends BaseMultiTenantIT {

    private static final String TOPIC_1 = "my-test-topic";
    private static final NewTopic NEW_TOPIC_1 = new NewTopic(TOPIC_1, 1, (short) 1);
    private static final String TOPIC_2 = "other-test-topic";
    private static final NewTopic NEW_TOPIC_2 = new NewTopic(TOPIC_2, 1, (short) 1);

    private static final String TOPIC_3 = "and-another-test-topic";
    private static final NewTopic NEW_TOPIC_3 = new NewTopic(TOPIC_3, 1, (short) 1);
    private static final String MY_KEY = "my-key";
    private static final String MY_VALUE = "my-value";

    @Test
    void createAndDeleteTopic(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster, this.certificateGenerator);

        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(TENANT_1_CLUSTER, this.clientConfig)) {
            var created = createTopics(admin, NEW_TOPIC_1, NEW_TOPIC_2);

            var topicMap = await().atMost(Duration.ofSeconds(5)).until(() -> admin.listTopics().namesToListings().get(),
                    n -> n.size() == 2);

            assertThat(topicMap).hasSize(2);
            assertThat(topicMap).hasEntrySatisfying(TOPIC_1,
                    allOf(matches(TopicListing::name, TOPIC_1), matches(TopicListing::topicId, created.topicId(TOPIC_1).get())));
            assertThat(topicMap).hasEntrySatisfying(TOPIC_1,
                    allOf(matches(TopicListing::name, TOPIC_1), matches(TopicListing::topicId, created.topicId(TOPIC_1).get())));
            assertThat(topicMap).hasEntrySatisfying(TOPIC_2, matches(TopicListing::name, TOPIC_2));

            // Delete by name
            var topics1 = TopicNameCollection.ofTopicNames(List.of(TOPIC_1));
            var deleted = deleteTopics(admin, topics1);
            assertThat(deleted.topicNameValues().keySet()).containsAll(topics1.topicNames());

            // Delete by id
            var topics2 = TopicCollection.ofTopicIds(List.of(created.topicId(TOPIC_2).get()));
            deleted = deleteTopics(admin, topics2);
            assertThat(deleted.topicIdValues().keySet()).containsAll(topics2.topicIds());
        }
    }

    @Test
    void describeTopic(KafkaCluster cluster) {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(TENANT_1_CLUSTER, this.clientConfig)) {
            var created = createTopics(admin, NEW_TOPIC_1);

            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().untilAsserted(() -> {
                var describeTopicsResult = admin.describeTopics(TopicNameCollection.ofTopicNames(List.of(TOPIC_1)));
                var topicMap = describeTopicsResult.allTopicNames().get();
                assertThat(topicMap).hasEntrySatisfying(TOPIC_1,
                        allOf(matches(TopicDescription::name, TOPIC_1), matches(TopicDescription::topicId, created.topicId(TOPIC_1).get())));
            });
        }
    }

    @Test
    void produceOne(KafkaCluster cluster) {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = buildTester(config)) {
            final String topicName = tester.createTopic(TENANT_1_CLUSTER);
            produceAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER, Stream.of(new ProducerRecord<>(topicName, MY_KEY, MY_VALUE)), Optional.empty());
        }
    }

    private KroxyliciousTester buildTester(ConfigurationBuilder config) {
        return KroxyliciousTesters.newBuilder(config)
                .setTrustStoreLocation(this.certificateGenerator.getTrustStoreLocation())
                .setTrustStorePassword(this.certificateGenerator.getPassword())
                .createDefaultKroxyliciousTester();
    }

    @Test
    void consumeOne(KafkaCluster cluster) {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = buildTester(config)) {
            var groupId = testInfo.getDisplayName();
            final String topicName = tester.createTopic(TENANT_1_CLUSTER);
            produceAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER, Stream.of(new ProducerRecord<>(topicName, MY_KEY, MY_VALUE)), Optional.empty());
            consumeAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER, topicName, groupId, new LinkedList<>(List.of(matchesRecord(topicName, MY_KEY, MY_VALUE))),
                    false);
        }
    }

    @Test
    void consumeOneAndOffsetCommit(KafkaCluster cluster) {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = buildTester(config)) {
            var groupId = testInfo.getDisplayName();
            final String topicName = tester.createTopic(TENANT_1_CLUSTER);
            produceAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER,
                    Stream.of(new ProducerRecord<>(topicName, MY_KEY, "1"), new ProducerRecord<>(topicName, MY_KEY, "2"), inCaseOfFailure()), Optional.empty());
            consumeAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER, topicName, groupId, new LinkedList<>(List.of(matchesRecord(topicName, MY_KEY, "1"))), true);
            consumeAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER, topicName, groupId, new LinkedList<>(List.of(matchesRecord(topicName, MY_KEY, "2"))), true);
        }
    }

    @Test
    void alterOffsetCommit(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = buildTester(config);
                var admin = tester.admin(TENANT_1_CLUSTER, this.clientConfig)) {
            var groupId = testInfo.getDisplayName();
            final String topicName = tester.createTopic(TENANT_1_CLUSTER);
            produceAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER,
                    Stream.of(new ProducerRecord<>(topicName, MY_KEY, "1"), new ProducerRecord<>(topicName, MY_KEY, "2"), inCaseOfFailure()), Optional.empty());
            consumeAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER, topicName, groupId, new LinkedList<>(List.of(matchesRecord(topicName, MY_KEY, "1"))), true);
            var rememberedOffsets = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            consumeAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER, topicName, groupId, new LinkedList<>(List.of(matchesRecord(topicName, MY_KEY, "2"))), true);

            admin.alterConsumerGroupOffsets(groupId, rememberedOffsets).all().get();
            consumeAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER, topicName, groupId, new LinkedList<>(List.of(matchesRecord(topicName, MY_KEY, "2"))), true);
        }
    }

    @Test
    void deleteConsumerGroupOffsets(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = buildTester(config);
                var admin = tester.admin(TENANT_1_CLUSTER, this.clientConfig)) {
            var groupId = testInfo.getDisplayName();
            final String topicName = tester.createTopic(TENANT_1_CLUSTER);
            produceAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER, Stream.of(new ProducerRecord<>(topicName, MY_KEY, "1"), inCaseOfFailure()), Optional.empty());
            consumeAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER, topicName, groupId, new LinkedList<>(List.of(matchesRecord(topicName, MY_KEY, "1"))), true);

            admin.deleteConsumerGroupOffsets(groupId, Set.of(new TopicPartition(topicName, 0))).all().get();
            consumeAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER, topicName, groupId, new LinkedList<>(List.of(matchesRecord(topicName, MY_KEY, "1"))), true);
        }
    }

    @Test
    void tenantTopicIsolation(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = kroxyliciousTester(config);
                var adminTenant1 = tester.admin(TENANT_1_CLUSTER, this.clientConfig);
                var adminTenant2 = tester.admin(TENANT_2_CLUSTER, this.clientConfig)) {
            createTopics(adminTenant1, NEW_TOPIC_1);
            createTopics(adminTenant2, NEW_TOPIC_2, NEW_TOPIC_3);

            verifyTenant(adminTenant1, TOPIC_1);
            verifyTenant(adminTenant2, TOPIC_2, TOPIC_3);
        }
    }

    @ParameterizedTest
    @EnumSource
    void tenantConsumeWithGroup(ConsumerStyle consumerStyle, KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(TENANT_1_CLUSTER, this.clientConfig)) {
            createTopics(admin, NEW_TOPIC_1);
            runConsumerInOrderToCreateGroup(tester, TENANT_1_CLUSTER, "Tenant1Group", NEW_TOPIC_1, consumerStyle, this.clientConfig);
            verifyConsumerGroupsWithList(admin, Set.of("Tenant1Group"));
        }
    }

    @ParameterizedTest
    @EnumSource
    void tenantGroupIsolation(ConsumerStyle consumerStyle, KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = kroxyliciousTester(config);
                var adminTenant1 = tester.admin(TENANT_1_CLUSTER, this.clientConfig);
                var adminTenant2 = tester.admin(TENANT_2_CLUSTER, this.clientConfig)) {
            createTopics(adminTenant1, NEW_TOPIC_1);
            runConsumerInOrderToCreateGroup(tester, TENANT_1_CLUSTER, "Tenant1Group", NEW_TOPIC_1, consumerStyle, this.clientConfig);

            createTopics(adminTenant2, NEW_TOPIC_1);
            runConsumerInOrderToCreateGroup(tester, TENANT_2_CLUSTER, "Tenant2Group", NEW_TOPIC_1, consumerStyle, this.clientConfig);
            verifyConsumerGroupsWithList(adminTenant2, Set.of("Tenant2Group"));
        }
    }

    @Test
    void describeGroup(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = kroxyliciousTester(config);
                var adminTenant1 = tester.admin(TENANT_1_CLUSTER, this.clientConfig);
                var adminTenant2 = tester.admin(TENANT_2_CLUSTER, this.clientConfig)) {
            createTopics(adminTenant1, NEW_TOPIC_1);
            runConsumerInOrderToCreateGroup(tester, TENANT_1_CLUSTER, "Tenant1Group", NEW_TOPIC_1, ConsumerStyle.ASSIGN, this.clientConfig);

            createTopics(adminTenant2, NEW_TOPIC_1);
            runConsumerInOrderToCreateGroup(tester, TENANT_2_CLUSTER, "Tenant2Group", NEW_TOPIC_1, ConsumerStyle.ASSIGN, this.clientConfig);

            verifyConsumerGroupsWithDescribe(adminTenant1, Set.of("Tenant1Group"), Set.of("Tenant2Group", "idontexist"));
            verifyConsumerGroupsWithDescribe(adminTenant2, Set.of("Tenant2Group"), Set.of("Tenant1Group", "idontexist"));
        }
    }

    @Test
    void produceInTransaction(KafkaCluster cluster) {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(TENANT_1_CLUSTER, this.clientConfig)) {
            createTopics(admin, NEW_TOPIC_1);
            produceAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER,
                    Stream.of(new ProducerRecord<>(TOPIC_1, MY_KEY, "1")),
                    Optional.of("12345"));
        }
    }

    @Test
    void produceAndConsumeInTransaction(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(TENANT_1_CLUSTER, this.clientConfig)) {
            // put some records in an input topic
            var inputTopic = "input";
            var outputTopic = "output";
            createTopics(admin, new NewTopic(inputTopic, 1, (short) 1),
                    new NewTopic(outputTopic, 1, (short) 1));
            produceAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER,
                    Stream.of(new ProducerRecord<>(inputTopic, MY_KEY, "1"),
                            new ProducerRecord<>(inputTopic, MY_KEY, "2"),
                            new ProducerRecord<>(inputTopic, MY_KEY, "3")),
                    Optional.empty());

            // now consume and from input and produce to output, using a transaction.
            var groupId = testInfo.getDisplayName();
            try (var consumer = getConsumerWithConfig(tester, TENANT_1_CLUSTER, groupId, this.clientConfig,
                    Map.of(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT)));
                    var producer = getProducerWithConfig(tester, Optional.of(TENANT_1_CLUSTER), clientConfig,
                            Map.of(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000, ProducerConfig.TRANSACTIONAL_ID_CONFIG, "12345"))) {
                producer.initTransactions();

                var topicPartitions = List.of(new TopicPartition(inputTopic, 0));
                consumer.assign(topicPartitions);

                String value = null;
                do {
                    var records = consumer.poll(Duration.ofSeconds(30));
                    for (var r : records) {
                        producer.beginTransaction();

                        var key = r.key();
                        value = r.value();
                        var send = producer.send(new ProducerRecord<>(outputTopic, 0, key, value));
                        send.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                        producer.sendOffsetsToTransaction(Map.of(new TopicPartition(r.topic(), r.partition()),
                                new OffsetAndMetadata(r.offset() + 1)), new ConsumerGroupMetadata(groupId));
                        producer.commitTransaction();
                    }
                } while (!"3".equals(value));
            }

            // now verify that output contains the expected values.
            consumeAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER, outputTopic, groupId, new LinkedList<>(
                    List.of(matchesRecord(outputTopic, MY_KEY, "1"),
                            matchesRecord(outputTopic, MY_KEY, "2"),
                            matchesRecord(outputTopic, MY_KEY, "3"))),
                    true);

        }
    }

    @Test
    void describeTransaction(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = kroxyliciousTester(config)) {
            try (var admin = tester.admin(TENANT_1_CLUSTER, this.clientConfig)) {
                createTopics(admin, NEW_TOPIC_1);
                var transactionalId = "12345";
                produceAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER,
                        Stream.of(new ProducerRecord<>(TOPIC_1, MY_KEY, "1")),
                        Optional.of(transactionalId));
                Awaitility.await().untilAsserted(() -> {
                    var describeTransactionsResult = admin.describeTransactions(List.of(transactionalId));
                    var transactionMap = describeTransactionsResult.all().get();
                    assertThat(transactionMap).hasEntrySatisfying(transactionalId,
                            allOf(matches(TransactionDescription::state, TransactionState.COMPLETE_COMMIT)));
                });
            }
        }
    }

    @Test
    void tenantTransactionalIdIsolation(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster, this.certificateGenerator);
        try (var tester = kroxyliciousTester(config);
                var adminTenant1 = tester.admin(TENANT_1_CLUSTER, this.clientConfig);
                var adminTenant2 = tester.admin(TENANT_2_CLUSTER, this.clientConfig)) {
            createTopics(adminTenant1, NEW_TOPIC_1);
            var tenant1TransactionId = "12345";
            produceAndAssert(tester, this.clientConfig, TENANT_1_CLUSTER,
                    Stream.of(new ProducerRecord<>(TOPIC_1, MY_KEY, "1")),
                    Optional.of(tenant1TransactionId));
            verifyTransactionsWithList(adminTenant1, Set.of(tenant1TransactionId));

            createTopics(adminTenant2, NEW_TOPIC_2);
            var tenant2TransactionId = "54321";
            produceAndAssert(tester, this.clientConfig, TENANT_2_CLUSTER,
                    Stream.of(new ProducerRecord<>(TOPIC_2, MY_KEY, "1")),
                    Optional.of(tenant2TransactionId));
            verifyTransactionsWithList(adminTenant2, Set.of(tenant2TransactionId));
        }
    }

    @Test
    void tenantPrefixUsingCustomSeparator(KafkaCluster cluster, KafkaAdminClient directAdmin) {
        var separator = ".";
        var config = getConfig(cluster, this.certificateGenerator, Map.of("prefixResourceNameSeparator", separator));
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(TENANT_1_CLUSTER, this.clientConfig)) {
            createTopics(admin, NEW_TOPIC_1);
        }

        var expectedTargetClusterTopicName = "%s%s%s".formatted(TENANT_1_CLUSTER, separator, TOPIC_1);
        await().atMost(Duration.ofSeconds(5)).ignoreExceptions().untilAsserted(() -> {
            var listTopicsResult = directAdmin.listTopics();
            var names = listTopicsResult.names().get();
            assertThat(names).contains(expectedTargetClusterTopicName);
        });

    }

    // ========================================================
    // UTILS
    // ========================================================

    private enum ConsumerStyle {
        ASSIGN
    }

    @NonNull
    private ProducerRecord<String, String> inCaseOfFailure() {
        return new ProducerRecord<>(TOPIC_1, MY_KEY, "unexpected - should never be consumed");
    }

    private void verifyConsumerGroupsWithDescribe(Admin admin, Set<String> expectedPresent, Set<String> expectedAbsent) throws Exception {
        var describedGroups = admin.describeConsumerGroups(expectedPresent).all().get();
        assertThat(describedGroups).allSatisfy((s, consumerGroupDescription) -> assertThat(consumerGroupDescription.groupState()).isNotIn(GroupState.DEAD));

        var describeFuture = admin.describeConsumerGroups(expectedAbsent).all();
        assertThatThrownBy(describeFuture::get)
                .hasRootCauseInstanceOf(GroupIdNotFoundException.class);
    }

    @NonNull
    private Map<String, ConsumerGroupDescription> retainKeySubset(Map<String, ConsumerGroupDescription> groups, Set<String> retainedKeys) {
        var copy = new HashMap<>(groups);
        copy.keySet().retainAll(retainedKeys);
        return copy;
    }

    private void runConsumerInOrderToCreateGroup(KroxyliciousTester tester, String virtualCluster, String groupId, NewTopic topic, ConsumerStyle consumerStyle,
                                                 Map<String, Object> clientConfig) {
        try (var consumer = getConsumerWithConfig(tester, virtualCluster, groupId, clientConfig, Map.of(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"))) {

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

    private void verifyConsumerGroupsWithList(Admin admin, Set<String> expectedGroupIds) throws Exception {
        var groups = admin.listConsumerGroups().all().get().stream().map(ConsumerGroupListing::groupId).toList();
        assertThat(groups).containsExactlyInAnyOrderElementsOf(expectedGroupIds);
    }

    private void verifyTransactionsWithList(Admin admin, Set<String> expectedTransactionalIds) throws Exception {
        var transactionalIds = admin.listTransactions().all().get().stream().map(TransactionListing::transactionalId).toList();
        assertThat(transactionalIds).containsExactlyInAnyOrderElementsOf(expectedTransactionalIds);
    }

    private void verifyTenant(Admin admin, String... expectedTopics) throws Exception {
        var topicListMap = await().atMost(Duration.ofSeconds(5)).until(() -> admin.listTopics().namesToListings().get(),
                m -> m.size() == expectedTopics.length);
        Arrays.stream(expectedTopics)
                .forEach(expectedTopic -> assertThat(topicListMap).hasEntrySatisfying(expectedTopic, allOf(matches(TopicListing::name, expectedTopic))));

        var describeTopicsResult = admin.describeTopics(TopicNameCollection.ofTopicNames(Arrays.stream(expectedTopics).toList()));
        var topicDescribeMap = describeTopicsResult.allTopicNames().get();
        assertThat(expectedTopics).hasSize(topicDescribeMap.size());
        Arrays.stream(expectedTopics)
                .forEach(expectedTopic -> assertThat(topicDescribeMap).hasEntrySatisfying(expectedTopic, allOf(matches(TopicDescription::name, expectedTopic))));
    }

    private static class PartitionAssignmentAwaitingRebalanceListener<K, V> implements ConsumerRebalanceListener {
        private final AtomicBoolean assigned = new AtomicBoolean();
        private final Consumer<K, V> consumer;

        PartitionAssignmentAwaitingRebalanceListener(Consumer<K, V> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            assigned.set(true);
        }

        public void awaitAssignment(Duration timeout) {
            await().atMost(timeout).until(() -> {
                consumer.poll(Duration.ofMillis(50));
                return assigned.get();
            });
        }
    }
}
