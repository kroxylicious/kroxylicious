package io.kroxylicious;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.filters.sasl.inspection.SaslInspection;
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
 * WIP usernamespace filter
 */
@ExtendWith(KafkaClusterExtension.class)
class UserNamespaceFilterIT {


    private enum ConsumerStyle {
        ASSIGN
    }

    @Test
    void describeGroup(@SaslMechanism(principals = { @SaslMechanism.Principal( user = "alice", password = "pwd") , @SaslMechanism.Principal( user = "bob", password = "pwd") }) KafkaCluster cluster, Topic topic1) throws Exception {

        var configBuilder = buildConfg(cluster);

        var aliceConfig = buildClientConfig("alice", "pwd");
        var bobConfig = buildClientConfig("bob", "pwd");
        try (var tester = kroxyliciousTester(configBuilder);
                var aliceAdmin = tester.admin(aliceConfig);
                var bobAdmin = tester.admin(bobConfig)) {
            runConsumerInOrderToCreateGroup(tester, "AliceGroup", topic1, ConsumerStyle.ASSIGN, aliceConfig);
            runConsumerInOrderToCreateGroup(tester, "BobGroup", topic1, ConsumerStyle.ASSIGN, bobConfig);

            verifyConsumerGroupsWithDescribe(aliceAdmin, Set.of("AliceGroup"), Set.of("BobGroup", "idontexist"));
            verifyConsumerGroupsWithDescribe(bobAdmin, Set.of("BobGroup"), Set.of("AliceGroup", "idontexist"));
        }
    }

    private static ConfigurationBuilder buildConfg(KafkaCluster cluster) {
        var configBuilder = KroxyliciousConfigUtils.proxy(cluster);

        var saslInspectionFilter = new NamedFilterDefinitionBuilder(
                SaslInspection.class.getName(),
                SaslInspection.class.getName());

        saslInspectionFilter.withConfig("enabledMechanisms", Set.of("PLAIN"));
        var saslInspection = saslInspectionFilter.build();

        var userNamespaceFilter = new NamedFilterDefinitionBuilder(
                UserNamespace.class.getName(),
                UserNamespace.class.getName());

//        userNamespaceFilter.withConfig("enabledMechanisms", "PLAIN");
        var userNamespace = userNamespaceFilter.build();


        configBuilder.addToFilterDefinitions(saslInspection, userNamespace)
                .addToDefaultFilters(saslInspection.name(), userNamespace.name());
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

    private static HashMap<String, Object> buildClientConfig(String username, String password) {
        var config = new HashMap<String, Object>();
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        config.put(SaslConfigs.SASL_JAAS_CONFIG,
                String.format("""
                        %s required username="%s" password="%s";""",
                        PlainLoginModule.class.getName(), username, password));
        config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        return config;
    }

    //
//    @Test
//    void fetchResponseFilter_shouldFindAndReplaceConfiguredWordInConsumedMessages(
//            @BrokerCluster final KafkaCluster kafkaCluster, final Topic topic) {
//        // configure the filters with the proxy
//        final var filterDefinition = new NamedFilterDefinitionBuilder("find-and-replace-consume-filter",
//                SampleFetchResponse.class.getName()).withConfig(FILTER_CONFIGURATION).build();
//        final var proxyConfiguration = KroxyliciousConfigUtils.proxy(kafkaCluster);
//        proxyConfiguration.addToFilterDefinitions(filterDefinition);
//        proxyConfiguration.addToDefaultFilters(filterDefinition.name());
//        // create proxy instance and a producer and a consumer connected to it
//        try (final var tester = KroxyliciousTesters.kroxyliciousTester(proxyConfiguration);
//                final var producer = tester.producer();
//                final var consumer = tester.consumer(Serdes.String(), Serdes.ByteArray(), CONSUMER_CONFIGURATION)) {
//            final ProducerRecord<String, String> producerRecord =
//                    new ProducerRecord<>(topic.name(), "This is foo!");
//            assertThat(producer.send(producerRecord)).succeedsWithin(TIMEOUT);
//            consumer.subscribe(List.of(topic.name()));
//            assertThat(consumer.poll(TIMEOUT).records(topic.name()))
//                    .singleElement()
//                    .extracting(ConsumerRecord::value)
//                    .extracting(String::new)
//                    .isEqualTo("This is bar!");
//        }
//    }

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
