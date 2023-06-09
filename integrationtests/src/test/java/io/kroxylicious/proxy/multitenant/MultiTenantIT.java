/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.multitenant;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicCollection.TopicNameCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.assertj.core.api.Condition;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.net.IntegrationTestInetAddressResolverProvider;
import io.kroxylicious.proxy.config.ClusterNetworkAddressConfigProviderDefinitionBuilder;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.allOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@ExtendWith(KafkaClusterExtension.class)
public class MultiTenantIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTenantIT.class);

    private static final String TOPIC_1 = "my-test-topic";
    private static final NewTopic NEW_TOPIC_1 = new NewTopic(TOPIC_1, 1, (short) 1);
    private static final String TOPIC_2 = "other-test-topic";
    private static final NewTopic NEW_TOPIC_2 = new NewTopic(TOPIC_2, 1, (short) 1);

    private static final String TOPIC_3 = "and-another-test-topic";
    private static final NewTopic NEW_TOPIC_3 = new NewTopic(TOPIC_3, 1, (short) 1);

    public static final String TENANT_1_CLUSTER = "foo";
    private static final HostPort TENANT_1_PROXY_ADDRESS = HostPort
            .parse(IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName(TENANT_1_CLUSTER, 9192));
    public static final String TENANT_2_CLUSTER = "bar";
    private static final HostPort TENANT_2_PROXY_ADDRESS = HostPort
            .parse(IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName(TENANT_2_CLUSTER, 9292));
    private static final String MY_KEY = "my-key";
    private static final String MY_VALUE = "my-value";
    private static final long FUTURE_TIMEOUT_SECONDS = 5L;

    private TestInfo testInfo;
    private KeytoolCertificateGenerator certificateGenerator;
    private Path clientTrustStore;
    @TempDir
    private Path certsDirectory;

    @BeforeEach
    public void beforeEach(TestInfo testInfo) throws Exception {
        this.testInfo = testInfo;
        // TODO: use a per-tenant server certificate.
        this.certificateGenerator = new KeytoolCertificateGenerator();
        this.certificateGenerator.generateSelfSignedCertificateEntry("test@redhat.com", IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName("*"),
                "KI", "RedHat", null, null, "US");
        this.clientTrustStore = certsDirectory.resolve("kafka.truststore.jks");
        this.certificateGenerator.generateTrustStore(this.certificateGenerator.getCertFilePath(), "client", clientTrustStore.toAbsolutePath().toString());
    }

    @Test
    public void createAndDeleteTopic(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);

        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(TENANT_1_CLUSTER, commonConfig(Map.of()))) {
            var created = createTopics(admin, List.of(NEW_TOPIC_1, NEW_TOPIC_2));

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
    public void describeTopic(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(TENANT_1_CLUSTER, commonConfig(Map.of()))) {
            var created = createTopics(admin, List.of(NEW_TOPIC_1));

            await().atMost(Duration.ofSeconds(5)).ignoreExceptions().untilAsserted(() -> {
                var describeTopicsResult = admin.describeTopics(TopicNameCollection.ofTopicNames(List.of(TOPIC_1)));
                var topicMap = describeTopicsResult.allTopicNames().get();
                assertThat(topicMap).hasEntrySatisfying(TOPIC_1,
                        allOf(matches(TopicDescription::name, TOPIC_1), matches(TopicDescription::topicId, created.topicId(TOPIC_1).get())));
            });
        }
    }

    private Map<String, Object> commonConfig(Map<String, Object> of) {
        var config = new HashMap<>(of);
        config.put(CommonClientConfigs.CLIENT_ID_CONFIG, testInfo.getDisplayName());
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.clientTrustStore.toAbsolutePath().toString());
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, certificateGenerator.getPassword());
        return config;
    }

    @Test
    public void produceOne(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config)) {
            createTopics(tester, TENANT_1_CLUSTER, List.of(NEW_TOPIC_1));
            produceAndVerify(tester, TENANT_1_CLUSTER, TOPIC_1, MY_KEY, MY_VALUE);
        }
    }

    @Test
    public void consumeOne(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config)) {
            var groupId = testInfo.getDisplayName();
            createTopics(tester, TENANT_1_CLUSTER, List.of(NEW_TOPIC_1));
            produceAndVerify(tester, TENANT_1_CLUSTER, TOPIC_1, MY_KEY, MY_VALUE);
            consumeAndVerify(tester, TENANT_1_CLUSTER, TOPIC_1, groupId, MY_KEY, MY_VALUE, false);
        }
    }

    @Test
    public void consumeOneAndOffsetCommit(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config)) {
            var groupId = testInfo.getDisplayName();
            createTopics(tester, TENANT_1_CLUSTER, List.of(NEW_TOPIC_1));
            produceAndVerify(tester, TENANT_1_CLUSTER,
                    Stream.of(new ProducerRecord<>(TOPIC_1, MY_KEY, "1"), new ProducerRecord<>(TOPIC_1, MY_KEY, "2"), inCaseOfFailure()), Optional.empty());
            consumeAndVerify(tester, TENANT_1_CLUSTER, TOPIC_1, groupId, MY_KEY, "1", true);
            consumeAndVerify(tester, TENANT_1_CLUSTER, TOPIC_1, groupId, MY_KEY, "2", true);
        }
    }

    @Test
    public void alterOffsetCommit(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(TENANT_1_CLUSTER, commonConfig(Map.of()))) {
            var groupId = testInfo.getDisplayName();
            createTopics(tester, TENANT_1_CLUSTER, List.of(NEW_TOPIC_1));
            produceAndVerify(tester, TENANT_1_CLUSTER,
                    Stream.of(new ProducerRecord<>(TOPIC_1, MY_KEY, "1"), new ProducerRecord<>(TOPIC_1, MY_KEY, "2"), inCaseOfFailure()), Optional.empty());
            consumeAndVerify(tester, TENANT_1_CLUSTER, TOPIC_1, groupId, MY_KEY, "1", true);
            var rememberedOffsets = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            consumeAndVerify(tester, TENANT_1_CLUSTER, TOPIC_1, groupId, MY_KEY, "2", true);

            admin.alterConsumerGroupOffsets(groupId, rememberedOffsets).all().get();
            consumeAndVerify(tester, TENANT_1_CLUSTER, TOPIC_1, groupId, MY_KEY, "2", true);
        }
    }

    @Test
    public void deleteConsumerGroupOffsets(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(TENANT_1_CLUSTER, commonConfig(Map.of()))) {
            var groupId = testInfo.getDisplayName();
            createTopics(tester, TENANT_1_CLUSTER, List.of(NEW_TOPIC_1));
            produceAndVerify(tester, TENANT_1_CLUSTER, Stream.of(new ProducerRecord<>(TOPIC_1, MY_KEY, "1"), inCaseOfFailure()), Optional.empty());
            consumeAndVerify(tester, TENANT_1_CLUSTER, TOPIC_1, groupId, MY_KEY, "1", true);

            admin.deleteConsumerGroupOffsets(groupId, Set.of(new TopicPartition(NEW_TOPIC_1.name(), 0))).all().get();
            consumeAndVerify(tester, TENANT_1_CLUSTER, TOPIC_1, groupId, MY_KEY, "1", true);
        }
    }

    @NotNull
    private static ProducerRecord<String, String> inCaseOfFailure() {
        return new ProducerRecord<>(TOPIC_1, MY_KEY, "unexpected - should never be consumed");
    }

    @Test
    public void tenantTopicIsolation(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config)) {
            createTopics(tester, TENANT_1_CLUSTER, List.of(NEW_TOPIC_1));
            createTopics(tester, TENANT_2_CLUSTER, List.of(NEW_TOPIC_2, NEW_TOPIC_3));

            verifyTenant(tester, TENANT_1_CLUSTER, TOPIC_1);
            verifyTenant(tester, TENANT_2_CLUSTER, TOPIC_2, TOPIC_3);
        }
    }

    private enum ConsumerStyle {
        ASSIGN,
        SUBSCRIBE
    }

    @ParameterizedTest
    @EnumSource
    public void tenantConsumeWithGroup(ConsumerStyle consumerStyle, KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config)) {
            createTopics(tester, TENANT_1_CLUSTER, List.of(NEW_TOPIC_1));
            runConsumerInOrderToCreateGroup(tester, TENANT_1_CLUSTER, "Tenant1Group", NEW_TOPIC_1, consumerStyle);
            verifyConsumerGroupsWithList(tester, TENANT_1_CLUSTER, Set.of("Tenant1Group"));
        }
    }

    @ParameterizedTest
    @EnumSource
    public void tenantGroupIsolation(ConsumerStyle consumerStyle, KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config)) {
            createTopics(tester, TENANT_1_CLUSTER, List.of(NEW_TOPIC_1));
            runConsumerInOrderToCreateGroup(tester, TENANT_1_CLUSTER, "Tenant1Group", NEW_TOPIC_1, consumerStyle);

            createTopics(tester, TENANT_2_CLUSTER, List.of(NEW_TOPIC_1));
            runConsumerInOrderToCreateGroup(tester, TENANT_2_CLUSTER, "Tenant2Group", NEW_TOPIC_1, consumerStyle);
            verifyConsumerGroupsWithList(tester, TENANT_2_CLUSTER, Set.of("Tenant2Group"));
        }
    }

    @Test
    public void describeGroup(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config)) {
            createTopics(tester, TENANT_1_CLUSTER, List.of(NEW_TOPIC_1));
            runConsumerInOrderToCreateGroup(tester, TENANT_1_CLUSTER, "Tenant1Group", NEW_TOPIC_1, ConsumerStyle.ASSIGN);

            createTopics(tester, TENANT_2_CLUSTER, List.of(NEW_TOPIC_1));
            runConsumerInOrderToCreateGroup(tester, TENANT_2_CLUSTER, "Tenant2Group", NEW_TOPIC_1, ConsumerStyle.ASSIGN);

            verifyConsumerGroupsWithDescribe(tester, TENANT_1_CLUSTER, Set.of("Tenant1Group"), Set.of("Tenant2Group", "idontexist"));
            verifyConsumerGroupsWithDescribe(tester, TENANT_2_CLUSTER, Set.of("Tenant2Group"), Set.of("Tenant1Group", "idontexist"));
        }
    }

    @Test
    public void produceInTransaction(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config)) {
            createTopics(tester, TENANT_1_CLUSTER, List.of(NEW_TOPIC_1));
            produceAndVerify(tester, TENANT_1_CLUSTER,
                    Stream.of(new ProducerRecord<>(TOPIC_1, MY_KEY, "1")),
                    Optional.of("12345"));
        }
    }

    @Test
    public void produceAndConsumeInTransaction(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config)) {
            // put some records in an input topic
            var inputTopic = "input";
            var outputTopic = "output";
            createTopics(tester, TENANT_1_CLUSTER, List.of(new NewTopic(inputTopic, 1, (short) 1),
                    new NewTopic(outputTopic, 1, (short) 1)));
            produceAndVerify(tester, TENANT_1_CLUSTER,
                    Stream.of(new ProducerRecord<>(inputTopic, MY_KEY, "1"),
                            new ProducerRecord<>(inputTopic, MY_KEY, "2"),
                            new ProducerRecord<>(inputTopic, MY_KEY, "3")),
                    Optional.empty());

            // now consume and from input and produce to output, using a transaction.
            var groupId = testInfo.getDisplayName();
            try (var consumer = tester.consumer(TENANT_1_CLUSTER, commonConfig(Map.of(
                    ConsumerConfig.GROUP_ID_CONFIG, groupId,
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString(),
                    ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT))));
                    var producer = tester.producer(TENANT_1_CLUSTER, commonConfig(Map.of(
                            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000,
                            ProducerConfig.TRANSACTIONAL_ID_CONFIG, "12345")))) {
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
            consumeAndVerify(tester, TENANT_1_CLUSTER, outputTopic, groupId, new LinkedList<>(
                    List.of(matchesRecord(outputTopic, MY_KEY, "1"),
                            matchesRecord(outputTopic, MY_KEY, "2"),
                            matchesRecord(outputTopic, MY_KEY, "3"))),
                    true);

        }
    }

    @Test
    public void describeTransaction(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config)) {
            try (var admin = tester.admin(TENANT_1_CLUSTER, commonConfig(Map.of()))) {
                createTopics(tester, TENANT_1_CLUSTER, List.of(NEW_TOPIC_1));
                var transactionalId = "12345";
                produceAndVerify(tester, TENANT_1_CLUSTER,
                        Stream.of(new ProducerRecord<>(TOPIC_1, MY_KEY, "1")),
                        Optional.of(transactionalId));

                var describeTransactionsResult = admin.describeTransactions(List.of(transactionalId));
                var transactionMap = describeTransactionsResult.all().get();
                assertThat(transactionMap).hasEntrySatisfying(transactionalId,
                        allOf(matches(TransactionDescription::state, TransactionState.COMPLETE_COMMIT)));
            }
        }
    }

    @Test
    public void tenantTransactionalIdIsolation(KafkaCluster cluster) throws Exception {
        var config = getConfig(cluster);
        try (var tester = kroxyliciousTester(config)) {
            createTopics(tester, TENANT_1_CLUSTER, List.of(NEW_TOPIC_1));
            var tenant1TransactionId = "12345";
            produceAndVerify(tester, TENANT_1_CLUSTER,
                    Stream.of(new ProducerRecord<>(TOPIC_1, MY_KEY, "1")),
                    Optional.of(tenant1TransactionId));
            verifyTransactionsWithList(tester, TENANT_1_CLUSTER, Set.of(tenant1TransactionId));

            createTopics(tester, TENANT_2_CLUSTER, List.of(NEW_TOPIC_2));
            var tenant2TransactionId = "54321";
            produceAndVerify(tester, TENANT_2_CLUSTER,
                    Stream.of(new ProducerRecord<>(TOPIC_2, MY_KEY, "1")),
                    Optional.of(tenant2TransactionId));
            verifyTransactionsWithList(tester, TENANT_2_CLUSTER, Set.of(tenant2TransactionId));
        }
    }

    private void verifyConsumerGroupsWithDescribe(KroxyliciousTester tester, String virtualCluster, Set<String> expectedPresent, Set<String> expectedAbsent)
            throws Exception {
        try (var admin = tester.admin(virtualCluster, commonConfig(Map.of()))) {
            var describedGroups = admin.describeConsumerGroups(Stream.concat(expectedPresent.stream(), expectedAbsent.stream()).toList()).all().get();
            assertThat(describedGroups).hasSize(expectedAbsent.size() + expectedPresent.size());

            var actualPresent = retainKeySubset(describedGroups, expectedPresent);
            var actualAbsent = retainKeySubset(describedGroups, expectedAbsent);

            // The consumer group comes back as "dead" when it doesn't exist, so we have to check that it's not dead/it is dead if we don't expect to see it
            assertThat(actualPresent).allSatisfy((s, consumerGroupDescription) -> assertThat(consumerGroupDescription.state()).isNotIn(ConsumerGroupState.DEAD));
            assertThat(actualAbsent).allSatisfy((s, consumerGroupDescription) -> assertThat(consumerGroupDescription.state()).isIn(ConsumerGroupState.DEAD));
        }
    }

    @NotNull
    private static Map<String, ConsumerGroupDescription> retainKeySubset(Map<String, ConsumerGroupDescription> groups, Set<String> retainedKeys) {
        var copy = new HashMap<>(groups);
        copy.keySet().retainAll(retainedKeys);
        return copy;
    }

    private void runConsumerInOrderToCreateGroup(KroxyliciousTester tester, String virtualCluster, String groupId, NewTopic topic, ConsumerStyle consumerStyle) {
        try (var consumer = tester.consumer(virtualCluster,
                commonConfig(Map.of(ConsumerConfig.GROUP_ID_CONFIG, groupId, ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, Boolean.FALSE.toString(),
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        OffsetResetStrategy.EARLIEST.toString(), ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")))) {

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

    private void verifyConsumerGroupsWithList(KroxyliciousTester tester, String virtualCluster, Set<String> expectedGroupIds) throws Exception {
        try (var admin = tester.admin(virtualCluster, commonConfig(Map.of()))) {
            var groups = admin.listConsumerGroups().all().get().stream().map(ConsumerGroupListing::groupId).toList();
            assertThat(groups).containsExactlyInAnyOrderElementsOf(expectedGroupIds);
        }
    }

    private void verifyTransactionsWithList(KroxyliciousTester tester, String virtualCluster, Set<String> expectedTransactionalIds) throws Exception {
        try (var admin = tester.admin(virtualCluster, commonConfig(Map.of()))) {
            var transactionalIds = admin.listTransactions().all().get().stream().map(TransactionListing::transactionalId).toList();
            assertThat(transactionalIds).containsExactlyInAnyOrderElementsOf(expectedTransactionalIds);
        }
    }

    private void verifyTenant(KroxyliciousTester tester, String virtualCluster, String... expectedTopics) throws Exception {
        try (var admin = tester.admin(virtualCluster, commonConfig(Map.of()))) {

            var listTopicsResult = admin.listTopics();

            var topicListMap = listTopicsResult.namesToListings().get();
            assertEquals(expectedTopics.length, topicListMap.size());
            Arrays.stream(expectedTopics)
                    .forEach(expectedTopic -> assertThat(topicListMap).hasEntrySatisfying(expectedTopic, allOf(matches(TopicListing::name, expectedTopic))));

            var describeTopicsResult = admin.describeTopics(TopicNameCollection.ofTopicNames(Arrays.stream(expectedTopics).toList()));
            var topicDescribeMap = describeTopicsResult.allTopicNames().get();
            assertEquals(expectedTopics.length, topicDescribeMap.size());
            Arrays.stream(expectedTopics)
                    .forEach(expectedTopic -> assertThat(topicDescribeMap).hasEntrySatisfying(expectedTopic, allOf(matches(TopicDescription::name, expectedTopic))));
        }
    }

    private void consumeAndVerify(KroxyliciousTester tester, String virtualCluster, String topicName, String groupId, String expectedKey, String expectedValue,
                                  boolean offsetCommit) {
        consumeAndVerify(tester, virtualCluster, topicName, groupId, new LinkedList<>(List.of(matchesRecord(topicName, expectedKey, expectedValue))), offsetCommit);
    }

    private void consumeAndVerify(KroxyliciousTester tester, String virtualCluster, String topicName, String groupId,
                                  Deque<Predicate<ConsumerRecord<String, String>>> expected, boolean offsetCommit) {
        try (var consumer = tester.consumer(virtualCluster,
                commonConfig(Map.of(ConsumerConfig.GROUP_ID_CONFIG, groupId, ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, Boolean.FALSE.toString(),
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        OffsetResetStrategy.EARLIEST.toString(), ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.format("%d", expected.size()))))) {

            var topicPartitions = List.of(new TopicPartition(topicName, 0));
            consumer.assign(topicPartitions);

            while (!expected.isEmpty()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));
                assertThat(records.partitions()).hasSizeGreaterThanOrEqualTo(1);
                records.forEach(r -> {
                    assertFalse(expected.isEmpty(), String.format("received unexpected record %s", r));
                    var predicate = expected.pop();
                    assertThat(r).matches(predicate, predicate.toString());
                });
            }

            if (offsetCommit) {
                consumer.commitSync(Duration.ofSeconds(5));
            }
        }
    }

    private void produceAndVerify(KroxyliciousTester tester, String virtualCluster, String topic, String key, String value) throws Exception {
        produceAndVerify(tester, virtualCluster, Stream.of(new ProducerRecord<>(topic, key, value)), Optional.empty());
    }

    private void produceAndVerify(KroxyliciousTester tester, String virtualCluster, Stream<ProducerRecord<String, String>> records, Optional<String> transactionalId)
            throws Exception {

        Map<String, Object> config = new HashMap<>(Map.of(
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
        transactionalId.ifPresent(tid -> config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId.get()));

        try (var producer = tester.producer(virtualCluster, commonConfig(config))) {
            transactionalId.ifPresent(u -> {
                producer.initTransactions();
                producer.beginTransaction();
            });

            records.forEach(rec -> {
                RecordMetadata recordMetadata = null;
                try {
                    recordMetadata = producer.send(rec).get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                catch (Exception e) {
                    fail("Caught: %s producing to %s", e.getMessage(), rec.topic());
                }
                assertNotNull(recordMetadata);
                assertNotNull(rec.topic(), recordMetadata.topic());
            });

            transactionalId.ifPresent(u -> {
                producer.commitTransaction();
            });

        }
    }

    private void createTopics(KroxyliciousTester tester, String virtualCluster, List<NewTopic> topics) throws Exception {
        try (var admin = tester.admin(virtualCluster, commonConfig(Map.of()))) {
            createTopics(admin, topics);
        }
    }

    private static CreateTopicsResult createTopics(Admin admin, List<NewTopic> topics) throws Exception {
        var created = admin.createTopics(topics);
        assertEquals(topics.size(), created.values().size());
        created.all().get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return created;
    }

    private DeleteTopicsResult deleteTopics(Admin admin, TopicCollection topics) throws Exception {
        var deleted = admin.deleteTopics(topics);
        deleted.all().get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return deleted;
    }

    private ConfigurationBuilder getConfig(KafkaCluster cluster) {
        return new ConfigurationBuilder()
                .addToVirtualClusters(TENANT_1_CLUSTER, new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .withClusterNetworkAddressConfigProvider(
                                new ClusterNetworkAddressConfigProviderDefinitionBuilder("PortPerBroker").withConfig("bootstrapAddress", TENANT_1_PROXY_ADDRESS)
                                        .build())
                        .withNewTls()
                        .withNewKeyStoreKey()
                        .withStoreFile(certificateGenerator.getKeyStoreLocation())
                        .withNewInlinePasswordStore(certificateGenerator.getPassword())
                        .endKeyStoreKey()
                        .endTls()
                        .build())
                .addToVirtualClusters(TENANT_2_CLUSTER, new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .withClusterNetworkAddressConfigProvider(
                                new ClusterNetworkAddressConfigProviderDefinitionBuilder("PortPerBroker").withConfig("bootstrapAddress", TENANT_2_PROXY_ADDRESS)
                                        .build())
                        .withNewTls()
                        .withNewKeyStoreKey()
                        .withStoreFile(certificateGenerator.getKeyStoreLocation())
                        .withNewInlinePasswordStore(certificateGenerator.getPassword())
                        .endKeyStoreKey()
                        .endTls()
                        .build())
                .addToFilters(new FilterDefinitionBuilder("ApiVersions").build())
                .addToFilters(new FilterDefinitionBuilder("MultiTenant").build());
    }

    @NotNull
    private <T, V> Condition<T> matches(Function<T, V> extractor, V expectedValue) {
        return new Condition<>(item -> Objects.equals(extractor.apply(item), expectedValue), "unexpected entry");
    }

    @NotNull
    private static <K, V> Predicate<ConsumerRecord<K, V>> matchesRecord(final String expectedTopic, final K expectedKey, final V expectedValue) {
        return new Predicate<>() {
            @Override
            public boolean test(ConsumerRecord<K, V> item) {
                return Objects.equals(item.topic(), expectedTopic) && Objects.equals(item.key(), expectedKey) && Objects.equals(
                        item.value(), expectedValue);
            }

            @Override
            public String toString() {
                return String.format("expected: key %s value %s", expectedKey, expectedValue);
            }
        };
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
