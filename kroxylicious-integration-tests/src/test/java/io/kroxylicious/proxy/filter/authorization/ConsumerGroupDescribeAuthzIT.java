/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.testing.kafka.common.ClientConfig;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerGroupDescribeAuthzIT extends AuthzIT {

    private static final String ALICE_TOPIC_NAME = "alice-topic";
    private static final String BOB_TOPIC_NAME = "bob-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(
            ALICE_TOPIC_NAME,
            BOB_TOPIC_NAME);
    public static final String ALICE_GROUP = "alice-group";
    public static final String BOB_GROUP = "bob-group";

    private Path rulesFile;

    private List<AclBinding> aclBindings;

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;

    @Name("kafkaClusterWithAuthz")
    @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = ALICE_GROUP)
    @ClientConfig(name = ConsumerConfig.GROUP_PROTOCOL_CONFIG, value = "consumer")
    @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest")
    static Consumer<byte[], byte[]> aliceKafkaClusterWithAuthzConsumer;

    @Name("kafkaClusterWithAuthz")
    @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = BOB_GROUP)
    @ClientConfig(name = ConsumerConfig.GROUP_PROTOCOL_CONFIG, value = "consumer")
    @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest")
    static Consumer<byte[], byte[]> bobKafkaClusterWithAuthzConsumer;

    @Name("kafkaClusterWithAuthz")
    static Producer<byte[], byte[]> kafkaClusterWithAuthzProducer;

    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = ALICE_GROUP)
    @ClientConfig(name = ConsumerConfig.GROUP_PROTOCOL_CONFIG, value = "consumer")
    @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest")
    @Name("kafkaClusterNoAuthz")
    static Consumer<byte[], byte[]> aliceKafkaClusterNoAuthzConsumer;

    @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = BOB_GROUP)
    @ClientConfig(name = ConsumerConfig.GROUP_PROTOCOL_CONFIG, value = "consumer")
    @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest")
    @Name("kafkaClusterNoAuthz")
    static Consumer<byte[], byte[]> bobKafkaClusterNoAuthzConsumer;

    @Name("kafkaClusterNoAuthz")
    static Producer<byte[], byte[]> kafkaClusterNoAuthzProducer;

    @BeforeAll
    void beforeAll() throws IOException, ExecutionException, InterruptedException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to DESCRIBE Topic with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TOPIC_NAME, BOB_TOPIC_NAME));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                // topic permissions
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, ALICE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, BOB_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                allowAllOnGroup(ALICE, ALICE_GROUP),
                allowAllOnGroup(ALICE, BOB_GROUP),
                allowAllOnGroup(BOB, ALICE_GROUP),
                allowAllOnGroup(BOB, BOB_GROUP),
                allowAllOnGroup(EVE, ALICE_GROUP),
                allowAllOnGroup(EVE, BOB_GROUP));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
        produceToAll(kafkaClusterWithAuthzProducer, ALL_TOPIC_NAMES_IN_TEST);
        establishConsumerGroup(aliceKafkaClusterWithAuthzConsumer, List.of(ALICE_TOPIC_NAME));
        establishConsumerGroup(bobKafkaClusterWithAuthzConsumer, List.of(BOB_TOPIC_NAME));
        produceToAll(kafkaClusterNoAuthzProducer, ALL_TOPIC_NAMES_IN_TEST);
        establishConsumerGroup(aliceKafkaClusterNoAuthzConsumer, List.of(ALICE_TOPIC_NAME));
        establishConsumerGroup(bobKafkaClusterNoAuthzConsumer, List.of(BOB_TOPIC_NAME));
    }

    private static void establishConsumerGroup(Consumer<byte[], byte[]> consumer, List<String> topicsToPoll) {
        consumer.subscribe(topicsToPoll);
        Awaitility.await().untilAsserted(() -> {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
            assertThat(records.count()).isGreaterThan(0);
        });
    }

    private static void produceToAll(Producer<byte[], byte[]> producer, List<String> topics) {
        for (String s : topics) {
            try {
                producer.send(new ProducerRecord<>(s, "key".getBytes(StandardCharsets.UTF_8), "val".getBytes(StandardCharsets.UTF_8))).get(5, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @AfterEach
    void tidyClusters() {
        aliceKafkaClusterNoAuthzConsumer.unsubscribe();
        bobKafkaClusterNoAuthzConsumer.unsubscribe();
        aliceKafkaClusterWithAuthzConsumer.unsubscribe();
        bobKafkaClusterWithAuthzConsumer.unsubscribe();
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    class ConsumerGroupDescribeEquivalence extends Equivalence<ConsumerGroupDescribeRequestData, ConsumerGroupDescribeResponseData> {

        private final ConsumerGroupDescribeRequestData requestData;

        ConsumerGroupDescribeEquivalence(short apiVersion, ConsumerGroupDescribeRequestData requestData) {
            super(apiVersion);
            this.requestData = requestData;
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.CONSUMER_GROUP_DESCRIBE;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public ConsumerGroupDescribeRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            return requestData;
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            clobber(jsonNodes);
            return prettyJsonString(jsonNodes);
        }

        private static void clobber(final JsonNode node) {
            if (node.isObject()) {
                clobberUuid((ObjectNode) node, "topicId");
                clobberString((ObjectNode) node, "memberId");
                clobberString((ObjectNode) node, "clientId");
                clobberInt((ObjectNode) node, "memberEpoch", 1);
                clobberInt((ObjectNode) node, "groupEpoch", 1);
                node.values().forEachRemaining(ConsumerGroupDescribeEquivalence::clobber);
            }
            if (node.isArray()) {
                node.forEach(ConsumerGroupDescribeEquivalence::clobber);
            }
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
        }

        @Override
        public void assertUnproxiedResponses(Map<String, ConsumerGroupDescribeResponseData> unproxiedResponsesByUser) {

        }

    }

    List<Arguments> shouldEnforceAccessToTopics() {
        ConsumerGroupDescribeRequestData aliceGroupRequest = new ConsumerGroupDescribeRequestData();
        aliceGroupRequest.setGroupIds(List.of(ALICE_GROUP));
        aliceGroupRequest.setIncludeAuthorizedOperations(false);
        ConsumerGroupDescribeRequestData bobGroupRequest = new ConsumerGroupDescribeRequestData();
        bobGroupRequest.setGroupIds(List.of(BOB_GROUP));
        bobGroupRequest.setIncludeAuthorizedOperations(false);
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.CONSUMER_GROUP_DESCRIBE),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.CONSUMER_GROUP_DESCRIBE))
                .boxed().flatMap(apiVersion -> Stream.of(
                        Arguments.argumentSet("api version " + apiVersion + " " + ALICE_GROUP + " request",
                                new ConsumerGroupDescribeEquivalence((short) (int) apiVersion, aliceGroupRequest)),
                        Arguments.argumentSet("api version " + apiVersion + " " + BOB_GROUP + " request",
                                new ConsumerGroupDescribeEquivalence((short) (int) apiVersion, bobGroupRequest))));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(ApiKeys.CONSUMER_GROUP_DESCRIBE.oldestVersion(), ApiKeys.CONSUMER_GROUP_DESCRIBE.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.CONSUMER_GROUP_DESCRIBE, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.CONSUMER_GROUP_DESCRIBE, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<ConsumerGroupDescribeRequestData, ConsumerGroupDescribeResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
