/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableConsumer;
import io.kroxylicious.testing.kafka.clients.CloseableProducer;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static io.kroxylicious.filter.authorization.OffsetFetchGroupBatchingEnforcement.FIRST_VERSION_USING_GROUP_BATCHING;
import static java.util.stream.Stream.concat;

public class OffsetFetchAuthzIT extends AuthzIT {

    public static final String EXISTING_TOPIC_NAME = "other-topic";
    public static final String GROUP_ID = "groupid";
    private Path rulesFile;

    private static final String ALICE_TO_DESCRIBE_TOPIC_NAME = "alice-new-topic";
    private static final String BOB_TO_DESCRIBE_TOPIC_NAME = "bob-new-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(EXISTING_TOPIC_NAME, ALICE_TO_DESCRIBE_TOPIC_NAME, BOB_TO_DESCRIBE_TOPIC_NAME);
    private static List<AclBinding> aclBindings;

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to DESCRIBE Topic with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TO_DESCRIBE_TOPIC_NAME, BOB_TO_DESCRIBE_TOPIC_NAME));

        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, ALICE_TO_DESCRIBE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, BOB_TO_DESCRIBE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                allowAllOnGroup(ALICE, GROUP_ID),
                allowAllOnGroup(BOB, GROUP_ID),
                allowAllOnGroup(EVE, GROUP_ID));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        produceAndConsumeToEstablishGroup(GROUP_ID, kafkaClusterWithAuthz);
        this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
        produceAndConsumeToEstablishGroup(GROUP_ID, kafkaClusterNoAuthz);
    }

    private static void produceAndConsumeToEstablishGroup(String groupId, KafkaCluster cluster) {
        Map<String, Object> producerConfig = cluster.getKafkaClientConfiguration(SUPER, "Super");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Map<String, Object> consumerConfig = cluster.getKafkaClientConfiguration(SUPER, "Super");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfig.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 0);
        consumerConfig.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 50);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (CloseableProducer<Object, Object> producer = new CloseableProducer<>(
                new KafkaProducer<>(producerConfig));
                CloseableConsumer<Object, Object> aSuper = new CloseableConsumer<>(new KafkaConsumer<>(consumerConfig))) {
            aSuper.subscribe(ALL_TOPIC_NAMES_IN_TEST);
            for (String topicName : ALL_TOPIC_NAMES_IN_TEST) {
                producer.send(new ProducerRecord<>(topicName, "value"));
            }
            producer.flush();
            int observedRecords = 0;
            while (observedRecords < ALL_TOPIC_NAMES_IN_TEST.size()) {
                ConsumerRecords<Object, Object> consumerRecords = aSuper.poll(Duration.ofMillis(50));
                observedRecords += consumerRecords.count();
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        Stream<Arguments> supportedVersionsBeforeGroupBatching = IntStream.range(AuthorizationFilter.minSupportedApiVersion(ApiKeys.OFFSET_FETCH),
                FIRST_VERSION_USING_GROUP_BATCHING)
                .mapToObj(apiVersion -> Arguments.argumentSet("api version before batching version " + apiVersion, new OffsetFetchEquivalence((short) apiVersion)));
        Stream<Arguments> supportedVersionsWithGroupBatching = IntStream.rangeClosed(8, AuthorizationFilter.maxSupportedApiVersion(ApiKeys.OFFSET_FETCH)).mapToObj(
                apiVersion -> Arguments.argumentSet("api version with batching version " + apiVersion, new OffsetFetchEquivalenceGroupBatching((short) apiVersion)));
        Stream<Arguments> unsupportedVersions = IntStream.rangeClosed(ApiKeys.OFFSET_FETCH.oldestVersion(), ApiKeys.OFFSET_FETCH.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.OFFSET_FETCH, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion, new UnsupportedApiVersion<>(ApiKeys.OFFSET_FETCH, (short) apiVersion)));
        Stream<Arguments> allSupported = concat(supportedVersionsBeforeGroupBatching, supportedVersionsWithGroupBatching);
        return concat(allSupported, unsupportedVersions).toList();
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<FetchRequestData, FetchResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

    class OffsetFetchEquivalence extends Equivalence<OffsetFetchRequestData, OffsetFetchResponseData> {

        OffsetFetchEquivalence(short apiVersion) {
            super(apiVersion);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse) {
            return prettyJsonString(jsonResponse);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {

        }

        @Override
        public void assertUnproxiedResponses(Map<String, OffsetFetchResponseData> unproxiedResponsesByUser) {

        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.OFFSET_FETCH;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public OffsetFetchRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            OffsetFetchRequestData fetchRequestData = new OffsetFetchRequestData();
            OffsetFetchRequestTopic topicA = createOffsetFetchTopic(ALICE_TO_DESCRIBE_TOPIC_NAME, 0, 20);
            OffsetFetchRequestTopic topicB = createOffsetFetchTopic(BOB_TO_DESCRIBE_TOPIC_NAME, 0, 20);
            OffsetFetchRequestTopic topicC = createOffsetFetchTopic(EXISTING_TOPIC_NAME, 0, 20);
            fetchRequestData.setGroupId(GROUP_ID);
            fetchRequestData.setTopics(List.of(topicA, topicB, topicC));
            return fetchRequestData;
        }
    }

    class OffsetFetchEquivalenceGroupBatching extends Equivalence<OffsetFetchRequestData, OffsetFetchResponseData> {

        OffsetFetchEquivalenceGroupBatching(short apiVersion) {
            super(apiVersion);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse) {
            return prettyJsonString(jsonResponse);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {

        }

        @Override
        public void assertUnproxiedResponses(Map<String, OffsetFetchResponseData> unproxiedResponsesByUser) {

        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.OFFSET_FETCH;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public OffsetFetchRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            OffsetFetchRequestData fetchRequestData = new OffsetFetchRequestData();
            OffsetFetchRequestTopics topicA = createOffsetFetchTopics(ALICE_TO_DESCRIBE_TOPIC_NAME, 0, 20);
            OffsetFetchRequestTopics topicB = createOffsetFetchTopics(BOB_TO_DESCRIBE_TOPIC_NAME, 0, 20);
            OffsetFetchRequestTopics topicC = createOffsetFetchTopics(EXISTING_TOPIC_NAME, 0, 20);
            OffsetFetchRequestData.OffsetFetchRequestGroup group = new OffsetFetchRequestData.OffsetFetchRequestGroup();
            group.setGroupId(GROUP_ID);
            group.topics().addAll(List.of(topicA, topicB, topicC));
            fetchRequestData.setGroups(List.of(group));
            return fetchRequestData;
        }
    }

    private static OffsetFetchRequestTopic createOffsetFetchTopic(String topicName, int... partitions) {
        OffsetFetchRequestTopic fetchTopic = new OffsetFetchRequestTopic();
        fetchTopic.setName(topicName);
        fetchTopic.setPartitionIndexes(Arrays.stream(partitions).boxed().toList());
        return fetchTopic;
    }

    private static OffsetFetchRequestTopics createOffsetFetchTopics(String topicName, int... partitions) {
        OffsetFetchRequestTopics fetchTopic = new OffsetFetchRequestTopics();
        fetchTopic.setName(topicName);
        fetchTopic.setPartitionIndexes(Arrays.stream(partitions).boxed().toList());
        return fetchTopic;
    }
}
