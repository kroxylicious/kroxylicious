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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicCollection.TopicNameCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

import io.kroxylicious.proxy.KroxyConfig;
import io.kroxylicious.proxy.VirtualClusterBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.proxy.Utils.startProxy;
import static org.assertj.core.api.Assertions.allOf;
import static org.assertj.core.api.Assertions.assertThat;
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

    private static final String PROXY_ADDRESS = "localhost:9192";
    private static final String TENANT1_PROXY_ADDRESS = "foo.multitenant.kafka:9192";
    private static final String TENANT2_PROXY_ADDRESS = "bar.multitenant.kafka:9193";
    private static final String MY_KEY = "my-key";
    private static final String MY_VALUE = "my-value";
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
        this.certificateGenerator.generateSelfSignedCertificateEntry("test@redhat.com", "*.multitenant.kafka", "KI", "RedHat", null, null, "US");
        this.clientTrustStore = certsDirectory.resolve("kafka.truststore.jks");
        this.certificateGenerator.generateTrustStore(this.certificateGenerator.getCertFilePath(), "client",
                clientTrustStore.toAbsolutePath().toString());
    }

    @Test
    public void createAndDeleteTopic(KafkaCluster cluster) throws Exception {
        String config = getConfig(cluster);

        try (var proxy = startProxy(config)) {
            try (var admin = Admin.create(commonConfig(TENANT1_PROXY_ADDRESS, Map.of()))) {
                var created = createTopics(admin, List.of(NEW_TOPIC_1, NEW_TOPIC_2));

                ListTopicsResult listTopicsResult = admin.listTopics();
                var topicMap = listTopicsResult.namesToListings().get();
                assertEquals(2, topicMap.size());
                assertThat(topicMap).hasSize(2);
                assertThat(topicMap).hasEntrySatisfying(TOPIC_1,
                        allOf(matches(TopicListing::name, TOPIC_1),
                                matches(TopicListing::topicId, created.topicId(TOPIC_1).get())));
                assertThat(topicMap).hasEntrySatisfying(TOPIC_1,
                        allOf(matches(TopicListing::name, TOPIC_1),
                                matches(TopicListing::topicId, created.topicId(TOPIC_1).get())));
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
    }

    @Test
    public void describeTopic(KafkaCluster cluster) throws Exception {
        String config = getConfig(cluster);
        try (var proxy = startProxy(config)) {
            try (var admin = Admin.create(commonConfig(TENANT1_PROXY_ADDRESS, Map.of()))) {
                var created = createTopics(admin, List.of(NEW_TOPIC_1));

                var describeTopicsResult = admin.describeTopics(TopicNameCollection.ofTopicNames(List.of(TOPIC_1)));
                var topicMap = describeTopicsResult.allTopicNames().get();
                assertThat(topicMap).hasEntrySatisfying(TOPIC_1,
                        allOf(matches(TopicDescription::name, TOPIC_1),
                                matches(TopicDescription::topicId, created.topicId(TOPIC_1).get())));
            }
        }
    }

    @Test
    public void publishOne(KafkaCluster cluster) throws Exception {
        String config = getConfig(cluster);
        try (var proxy = startProxy(config)) {
            createTopics(TENANT1_PROXY_ADDRESS, List.of(NEW_TOPIC_1));
            produceAndVerify(TENANT1_PROXY_ADDRESS, TOPIC_1, MY_KEY, MY_VALUE);
        }
    }

    @Test
    public void consumeOne(KafkaCluster cluster) throws Exception {
        String config = getConfig(cluster);
        try (var proxy = startProxy(config)) {
            var groupId = testInfo.getDisplayName();
            createTopics(TENANT1_PROXY_ADDRESS, List.of(NEW_TOPIC_1));
            produceAndVerify(TENANT1_PROXY_ADDRESS, TOPIC_1, MY_KEY, MY_VALUE);
            consumeAndVerify(TENANT1_PROXY_ADDRESS, TOPIC_1, groupId, MY_KEY, MY_VALUE, false);
        }
    }

    @Test
    public void consumeOneAndOffsetCommit(KafkaCluster cluster) throws Exception {
        String config = getConfig(cluster);
        try (var proxy = startProxy(config)) {
            var groupId = testInfo.getDisplayName();
            createTopics(TENANT1_PROXY_ADDRESS, List.of(NEW_TOPIC_1));
            produceAndVerify(TENANT1_PROXY_ADDRESS,
                    Stream.of(new ProducerRecord<>(TOPIC_1, MY_KEY, "1"), new ProducerRecord<>(TOPIC_1, MY_KEY, "2"), inCaseOfFailure()));
            consumeAndVerify(TENANT1_PROXY_ADDRESS, TOPIC_1, groupId, MY_KEY, "1", true);
            consumeAndVerify(TENANT1_PROXY_ADDRESS, TOPIC_1, groupId, MY_KEY, "2", true);
        }
    }

    @Test
    public void alterOffsetCommit(KafkaCluster cluster) throws Exception {
        String config = getConfig(cluster);
        try (var proxy = startProxy(config); var admin = Admin.create(commonConfig(TENANT1_PROXY_ADDRESS, Map.of()))) {
            var groupId = testInfo.getDisplayName();
            createTopics(TENANT1_PROXY_ADDRESS, List.of(NEW_TOPIC_1));
            produceAndVerify(TENANT1_PROXY_ADDRESS,
                    Stream.of(new ProducerRecord<>(TOPIC_1, MY_KEY, "1"), new ProducerRecord<>(TOPIC_1, MY_KEY, "2"),
                            inCaseOfFailure()));
            consumeAndVerify(TENANT1_PROXY_ADDRESS, TOPIC_1, groupId, MY_KEY, "1", true);
            var rememberedOffsets = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            consumeAndVerify(TENANT1_PROXY_ADDRESS, TOPIC_1, groupId, MY_KEY, "2", true);

            admin.alterConsumerGroupOffsets(groupId, rememberedOffsets).all().get();
            consumeAndVerify(TENANT1_PROXY_ADDRESS, TOPIC_1, groupId, MY_KEY, "2", true);
        }
    }

    @Test
    public void deleteConsumerGroupOffsets(KafkaCluster cluster) throws Exception {
        String config = getConfig(cluster);
        try (var proxy = startProxy(config); var admin = Admin.create(commonConfig(TENANT1_PROXY_ADDRESS, Map.of()))) {
            var groupId = testInfo.getDisplayName();
            createTopics(TENANT1_PROXY_ADDRESS, List.of(NEW_TOPIC_1));
            produceAndVerify(TENANT1_PROXY_ADDRESS,
                    Stream.of(new ProducerRecord<>(TOPIC_1, MY_KEY, "1"), inCaseOfFailure()));
            consumeAndVerify(TENANT1_PROXY_ADDRESS, TOPIC_1, groupId, MY_KEY, "1", true);

            admin.deleteConsumerGroupOffsets(groupId, Set.of(new TopicPartition(NEW_TOPIC_1.name(), 0))).all().get();
            consumeAndVerify(TENANT1_PROXY_ADDRESS, TOPIC_1, groupId, MY_KEY, "1", true);
        }
    }

    @NotNull
    private static ProducerRecord<String, String> inCaseOfFailure() {
        return new ProducerRecord<>(TOPIC_1, MY_KEY, "unexpected - should never be consumed");
    }

    @Test
    public void tenantTopicIsolation(KafkaCluster cluster) throws Exception {
        String config = getConfig(cluster);
        try (var proxy = startProxy(config)) {
            createTopics(TENANT1_PROXY_ADDRESS, List.of(NEW_TOPIC_1));
            createTopics(TENANT2_PROXY_ADDRESS, List.of(NEW_TOPIC_2, NEW_TOPIC_3));

            verifyTenant(TENANT1_PROXY_ADDRESS, TOPIC_1);
            verifyTenant(TENANT2_PROXY_ADDRESS, TOPIC_2, TOPIC_3);
        }
    }

    private enum ConsumerStyle {
        ASSIGN,
        SUBSCRIBE
    }

    @ParameterizedTest
    @EnumSource
    public void tenantConsumeWithGroup(ConsumerStyle consumerStyle, KafkaCluster cluster) throws Exception {
        String config = getConfig(cluster);
        try (var proxy = startProxy(config)) {
            createTopics(TENANT1_PROXY_ADDRESS, List.of(NEW_TOPIC_1));
            runConsumerInOrderToCreateGroup(TENANT1_PROXY_ADDRESS, "Tenant1Group", NEW_TOPIC_1, consumerStyle);
            verifyConsumerGroupsWithList(TENANT1_PROXY_ADDRESS, Set.of("Tenant1Group"));
        }
    }

    @ParameterizedTest
    @EnumSource
    public void tenantGroupIsolation(ConsumerStyle consumerStyle, KafkaCluster cluster) throws Exception {
        String config = getConfig(cluster);
        try (var proxy = startProxy(config)) {
            createTopics(TENANT1_PROXY_ADDRESS, List.of(NEW_TOPIC_1));
            runConsumerInOrderToCreateGroup(TENANT1_PROXY_ADDRESS, "Tenant1Group", NEW_TOPIC_1, consumerStyle);

            createTopics(TENANT2_PROXY_ADDRESS, List.of(NEW_TOPIC_1));
            runConsumerInOrderToCreateGroup(TENANT2_PROXY_ADDRESS, "Tenant2Group", NEW_TOPIC_1, consumerStyle);
            verifyConsumerGroupsWithList(TENANT2_PROXY_ADDRESS, Set.of("Tenant2Group"));
        }
    }

    @Test
    public void describeGroup(KafkaCluster cluster) throws Exception {
        String config = getConfig(cluster);
        try (var proxy = startProxy(config)) {
            createTopics(TENANT1_PROXY_ADDRESS, List.of(NEW_TOPIC_1));
            runConsumerInOrderToCreateGroup(TENANT1_PROXY_ADDRESS, "Tenant1Group", NEW_TOPIC_1, ConsumerStyle.ASSIGN);

            createTopics(TENANT2_PROXY_ADDRESS, List.of(NEW_TOPIC_1));
            runConsumerInOrderToCreateGroup(TENANT2_PROXY_ADDRESS, "Tenant2Group", NEW_TOPIC_1, ConsumerStyle.ASSIGN);

            verifyConsumerGroupsWithDescribe(TENANT1_PROXY_ADDRESS, Set.of("Tenant1Group"), Set.of("Tenant2Group", "idontexist"));
            verifyConsumerGroupsWithDescribe(TENANT2_PROXY_ADDRESS, Set.of("Tenant2Group"), Set.of("Tenant1Group", "idontexist"));
        }
    }

    private void verifyConsumerGroupsWithDescribe(String proxyAddress, Set<String> expectedPresent, Set<String> expectedAbsent) throws Exception {
        try (var admin = Admin.create(commonConfig(proxyAddress, Map.of()))) {
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

    private void runConsumerInOrderToCreateGroup(String proxyAddress, String groupId, NewTopic topic, ConsumerStyle consumerStyle) throws Exception {
        try (var consumer = new KafkaConsumer<String, String>(commonConfig(proxyAddress, Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, Boolean.FALSE.toString(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)))) {

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

    private void verifyConsumerGroupsWithList(String proxyAddress, Set<String> expectedGroupIds) throws Exception {
        try (var admin = Admin.create(commonConfig(proxyAddress, Map.of()))) {
            var groups = admin.listConsumerGroups().all().get().stream().map(ConsumerGroupListing::groupId).toList();
            assertThat(groups).containsExactlyInAnyOrderElementsOf(expectedGroupIds);
        }
    }

    private void verifyTenant(String address, String... expectedTopics) throws Exception {
        try (var admin = Admin.create(commonConfig(address, Map.of()))) {

            var listTopicsResult = admin.listTopics();

            var topicListMap = listTopicsResult.namesToListings().get();
            assertEquals(expectedTopics.length, topicListMap.size());
            Arrays.stream(expectedTopics).forEach(
                    expectedTopic -> assertThat(topicListMap).hasEntrySatisfying(expectedTopic,
                            allOf(matches(TopicListing::name, expectedTopic))));

            var describeTopicsResult = admin.describeTopics(TopicNameCollection.ofTopicNames(Arrays.stream(expectedTopics).toList()));
            var topicDescribeMap = describeTopicsResult.allTopicNames().get();
            assertEquals(expectedTopics.length, topicDescribeMap.size());
            Arrays.stream(expectedTopics).forEach(expectedTopic -> assertThat(topicDescribeMap).hasEntrySatisfying(expectedTopic,
                    allOf(matches(TopicDescription::name, expectedTopic))));
        }
    }

    private void consumeAndVerify(String address, String topicName, String groupId, String expectedKey, String expectedValue, boolean offsetCommit) {
        consumeAndVerify(address, topicName, groupId, new LinkedList<>(List.of(matchesRecord(topicName, expectedKey, expectedValue))), offsetCommit);
    }

    private void consumeAndVerify(String address, String topicName, String groupId, Deque<Predicate<ConsumerRecord<String, String>>> expected, boolean offsetCommit) {
        try (var consumer = new KafkaConsumer<String, String>(commonConfig(address, Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, Boolean.FALSE.toString(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString(),
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.format("%d", expected.size()),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)))) {

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

    private void produceAndVerify(String address, String topic, String key, String value) throws Exception {
        produceAndVerify(address, Stream.of(new ProducerRecord<>(topic, key, value)));
    }

    private void produceAndVerify(String address, Stream<ProducerRecord<String, String>> records) throws Exception {

        try (var producer = new KafkaProducer<String, String>(commonConfig(address, Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000)))) {

            records.forEach(rec -> {
                RecordMetadata recordMetadata;
                try {
                    recordMetadata = producer.send(rec).get();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                assertNotNull(recordMetadata);
                assertNotNull(rec.topic(), recordMetadata.topic());
            });

        }
    }

    private void createTopics(String address, List<NewTopic> topics) throws Exception {
        try (var admin = Admin.create(commonConfig(address, Map.of()))) {
            createTopics(admin, topics);
        }
    }

    private static CreateTopicsResult createTopics(Admin admin, List<NewTopic> topics) throws Exception {
        var created = admin.createTopics(topics);
        assertEquals(topics.size(), created.values().size());
        created.all().get();
        return created;
    }

    private DeleteTopicsResult deleteTopics(String address, TopicCollection topics) throws Exception {
        try (var admin = Admin.create(commonConfig(
                address, Map.of()))) {
            return deleteTopics(admin, topics);
        }
    }

    private DeleteTopicsResult deleteTopics(Admin admin, TopicCollection topics) throws Exception {
        var deleted = admin.deleteTopics(topics);
        deleted.all().get();
        return deleted;
    }

    @NotNull
    private Map<String, Object> commonConfig(String address, Map<String, Object> m) {
        var config = new HashMap<String, Object>();
        config.putAll(m);
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, address);
        config.put(CommonClientConfigs.CLIENT_ID_CONFIG, testInfo.getDisplayName());
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.clientTrustStore.toAbsolutePath().toString());
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, certificateGenerator.getPassword());
        return config;
    }

    private String getConfig(KafkaCluster cluster) {
        return KroxyConfig.builder()
                .addToVirtualClusters("foo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .withNewClusterEndpointProvider()
                        .withType("StaticCluster")
                        .withConfig(Map.of("bootstrapAddress", TENANT1_PROXY_ADDRESS))
                        .endClusterEndpointProvider()
                        .withKeyPassword(certificateGenerator.getPassword())
                        .withKeyStoreFile(certificateGenerator.getKeyStoreLocation())
                        .build())
                .addToVirtualClusters("bar", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                        .endTargetCluster()
                        .withNewClusterEndpointProvider()
                        .withType("StaticCluster")
                        .withConfig(Map.of("bootstrapAddress", TENANT2_PROXY_ADDRESS))
                        .endClusterEndpointProvider()
                        .withKeyPassword(certificateGenerator.getPassword())
                        .withKeyStoreFile(certificateGenerator.getKeyStoreLocation())
                        .build())
                .addNewFilter().withType("ApiVersions").endFilter()
                .addNewFilter().withType("BrokerAddress").endFilter()
                .addNewFilter().withType("MultiTenant").endFilter()
                .build().toYaml();
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
                var rec = ((ConsumerRecord<K, V>) item);
                return Objects.equals(rec.topic(), expectedTopic) && Objects.equals(rec.key(), expectedKey) && Objects.equals(rec.value(), expectedValue);
            }

            @Override
            public String toString() {
                return String.format("expected: key %s value %s", expectedKey, expectedValue);
            }
        };
    }

    private static class PartitionAssignmentAwaitingRebalanceListener<K, V> implements ConsumerRebalanceListener {
        private final AtomicBoolean assigned = new AtomicBoolean();
        private final KafkaConsumer<K, V> consumer;

        PartitionAssignmentAwaitingRebalanceListener(KafkaConsumer<K, V> consumer) {
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
