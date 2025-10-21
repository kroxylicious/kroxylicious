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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.kroxylicious.filter.entityisolation.EntityIsolation;
import io.kroxylicious.filter.entityisolation.PrincipalEntityNameMapperService;
import io.kroxylicious.filter.sasl.inspection.SaslInspection;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for Entity Isolation Filter.
 * TODO: transactional id tests, share groups
 */
@ExtendWith(KafkaClusterExtension.class)
class EntityIsolationIT {

    public static final String CONSUMER_GROUP_NAME = "mygroup";

    private enum ConsumerStyle {
        ASSIGN,
        SUBSCRIBE
    }

    /**
     * Group isolation - describe groups.
     * <br/>
     * Alice and Bob both consume from the same topic using distinct own group names.
     * Test uses describeConsumerGroups to ensure that Alice only sees her group and Bob only sees his.
     * @param cluster broker
     * @param topic topic
     */
    @ParameterizedTest
    @EnumSource(value = ConsumerStyle.class)
    void describeGroupMaintainsGroupIsolation(ConsumerStyle consumerStyle, @SaslMechanism(principals = { @SaslMechanism.Principal(user = "alice", password = "pwd"),
            @SaslMechanism.Principal(user = "bob", password = "pwd") }) KafkaCluster cluster, Topic topic) {

        checkGroupIsolationMaintained(consumerStyle, cluster, topic, true);
    }

    /**
     * Group isolation - list groups.
     * <br/>
     * Alice and Bob both consumer from the same topic using distinct own group names.
     * Test uses listGroups to ensure that Alice only sees her group and Bob only sees his.
     * @param cluster broker
     * @param topic topic
     */
    @ParameterizedTest
    @EnumSource(value = ConsumerStyle.class)
    void listGroupMaintainsGroupIsolation(ConsumerStyle consumerStyle, @SaslMechanism(principals = { @SaslMechanism.Principal(user = "alice", password = "pwd"),
            @SaslMechanism.Principal(user = "bob", password = "pwd") }) KafkaCluster cluster, Topic topic) {

        checkGroupIsolationMaintained(consumerStyle, cluster, topic, false);
    }

    private void checkGroupIsolationMaintained(ConsumerStyle consumerStyle, KafkaCluster cluster, Topic topic, boolean useDescribe) {
        var configBuilder = buildProxyConfig(cluster);

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
     *
     * @param cluster cluster
     * @param topic topic
     */
    @Test
    void consumerGroupOffsetMaintainGroupIsolation(@SaslMechanism(principals = { @SaslMechanism.Principal(user = "alice", password = "pwd"),
            @SaslMechanism.Principal(user = "bob", password = "pwd") }) KafkaCluster cluster, Topic topic) {

        var configBuilder = buildProxyConfig(cluster);
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

    @Test
    void listConsumerGroupOffsets(@SaslMechanism(principals = { @SaslMechanism.Principal(user = "alice", password = "pwd") }) KafkaCluster cluster, Topic topic) {

        var configBuilder = buildProxyConfig(cluster);
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
                        .extractingByKey(CONSUMER_GROUP_NAME, InstanceOfAssertFactories.map(TopicPartition.class, OffsetAndMetadata.class))
                        .extractingByKey(new TopicPartition(topic.name(), 0))
                        .satisfies(oam -> {
                            assertThat(oam.offset()).isEqualTo(1);
                        });
            }
        }
    }

    @Test
    void deleteConsumerGroups(@SaslMechanism(principals = { @SaslMechanism.Principal(user = "alice", password = "pwd") }) KafkaCluster cluster, Topic topic) {
        var configBuilder = buildProxyConfig(cluster);

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

    private static ConfigurationBuilder buildProxyConfig(KafkaCluster cluster) {
        var configBuilder = KroxyliciousConfigUtils.proxy(cluster);

        var saslInspectionFilter = new NamedFilterDefinitionBuilder(
                SaslInspection.class.getName(),
                SaslInspection.class.getName());

        saslInspectionFilter.withConfig("enabledMechanisms", Set.of("PLAIN"));
        var saslInspection = saslInspectionFilter.build();

        var entityIsolationFilter = new NamedFilterDefinitionBuilder(
                EntityIsolation.class.getName(),
                EntityIsolation.class.getName());

        entityIsolationFilter.withConfig("resourceTypes", List.of("GROUP_ID"),
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
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, ConsumerGroupDescription.class))
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

    private void verifyConsumerGroupsWithList(Admin admin, Set<String> expected) {
        assertThat(admin.listGroups().all())
                .succeedsWithin(Duration.ofSeconds(5))
                .asInstanceOf(InstanceOfAssertFactories.collection(GroupListing.class))
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

        public void awaitAssignment(Duration timeout) {
            await().atMost(timeout).until(() -> {
                consumer.poll(Duration.ofMillis(50));
                return assigned.get();
            });
        }
    }
}
