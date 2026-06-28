/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableConsumer;
import io.kroxylicious.testing.kafka.clients.CloseableProducer;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static io.kroxylicious.filter.authorization.OffsetFetchEnforcement.FIRST_VERSION_USING_GROUP_BATCHING;

/**
 * OffsetFetch requests DESCRIBE permissions for the groups in the request. This test isolates the
 * Group authorization, allowing all operations on topics for all users.
 */
class OffsetFetchGroupAuthzIT extends AuthzIT {

    public static final short FIRST_VERSION_SUPPORTING_ALL_TOPICS = 2;

    public static final String EXISTING_TOPIC_NAME = "other-topic";
    private static final String ALICE_TO_DESCRIBE_GROUP = "alice-group";
    private static final String BOB_TO_DESCRIBE_GROUP = "bob-group";
    public static final List<String> ALL_GROUP_IDS = List.of(ALICE_TO_DESCRIBE_GROUP, BOB_TO_DESCRIBE_GROUP);
    private Path rulesFile;
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(EXISTING_TOPIC_NAME);
    private static List<AclBinding> aclBindings;

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import GroupResource as Group;
                allow User with name = "alice" to * Group with name = "%s";
                allow User with name = "bob" to DESCRIBE Group with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TO_DESCRIBE_GROUP, BOB_TO_DESCRIBE_GROUP));

        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, ALICE_TO_DESCRIBE_GROUP, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, BOB_TO_DESCRIBE_GROUP, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                allowAllOnTopic(EXISTING_TOPIC_NAME, ALICE),
                allowAllOnTopic(EXISTING_TOPIC_NAME, BOB),
                allowAllOnTopic(EXISTING_TOPIC_NAME, EVE));
    }

    private static AclBinding allowAllOnTopic(String topic, String user) {
        return new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                new AccessControlEntry("User:" + user, "*",
                        AclOperation.ALL, AclPermissionType.ALLOW));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        produceAndConsumeToEstablishGroup(kafkaClusterWithAuthz);
        this.topicIdsInProxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
        produceAndConsumeToEstablishGroup(kafkaClusterNoAuthz);
    }

    private static void produceAndConsumeToEstablishGroup(KafkaCluster cluster) {
        OffsetFetchGroupAuthzIT.ALL_GROUP_IDS.forEach(groupId -> {
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
        });
    }

    @AfterEach
    void tidyClusters() {
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        var result = new ArrayList<Arguments>();
        for (short apiVersion = AuthorizationFilter.minSupportedApiVersion(ApiKeys.OFFSET_FETCH); apiVersion <= AuthorizationFilter
                .maxSupportedApiVersion(ApiKeys.OFFSET_FETCH); apiVersion++) {
            if (AuthorizationFilter.isApiVersionSupported(ApiKeys.OFFSET_FETCH, apiVersion)) {
                if (apiVersion < FIRST_VERSION_USING_GROUP_BATCHING) {
                    for (String groupId : ALL_GROUP_IDS) {
                        if (apiVersion >= FIRST_VERSION_SUPPORTING_ALL_TOPICS) {
                            result.add(Arguments.argumentSet("api version " + apiVersion + " (before batching), all topics",
                                    new OffsetFetchEquivalence(apiVersion, true, groupId)));
                        }
                        result.add(Arguments.argumentSet("api version " + apiVersion + " (before batching), given topics",
                                new OffsetFetchEquivalence(apiVersion, false, groupId)));
                    }
                }
                else {
                    result.add(Arguments.argumentSet("api version " + apiVersion + " (with batching), all topics",
                            new OffsetFetchEquivalenceGroupBatching(apiVersion, true)));
                    result.add(Arguments.argumentSet("api version " + apiVersion + " (with batching), given topics",
                            new OffsetFetchEquivalenceGroupBatching(apiVersion, false)));
                }
            }
            else {
                result.add(Arguments.argumentSet("unsupported version " + apiVersion, new UnsupportedApiVersion<>(ApiKeys.OFFSET_FETCH, (short) apiVersion)));
            }

        }
        return result;
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

        private final boolean allTopics;
        private String groupId;

        OffsetFetchEquivalence(short apiVersion, boolean allTopics, String groupId) {
            super(apiVersion);
            this.allTopics = allTopics;
            this.groupId = Objects.requireNonNull(groupId);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse) {
            sortArray(jsonResponse, "topics", "name");
            return prettyJsonString(jsonResponse);
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
            OffsetFetchRequestTopic topic = createOffsetFetchTopic(EXISTING_TOPIC_NAME, 0, 20);
            fetchRequestData.setGroupId(groupId);
            if (allTopics) {
                fetchRequestData.setTopics(null);
            }
            else {
                fetchRequestData.setTopics(List.of(topic));
            }
            return fetchRequestData;
        }
    }

    class OffsetFetchEquivalenceGroupBatching extends Equivalence<OffsetFetchRequestData, OffsetFetchResponseData> {

        private final boolean allTopics;

        OffsetFetchEquivalenceGroupBatching(short apiVersion, boolean allTopics) {
            super(apiVersion);
            this.allTopics = allTopics;
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse) {
            ArrayNode groups = (ArrayNode) jsonResponse.path("groups");
            sortArray(jsonResponse, "groups", "groupId");
            for (var group : groups) {
                JsonNode topics = group.get("topics");
                if (topics.isArray()) {
                    for (JsonNode topic : topics) {
                        if (topic instanceof ObjectNode objectNode) {
                            replaceTopicIdWithName(cluster, objectNode, "topicId");
                        }
                    }
                }
                sortArray((ObjectNode) group, "topics", "name", TEST_MAPPED_TOPIC_NAME_PROPERTY);
            }
            return prettyJsonString(jsonResponse);
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
            OffsetFetchRequestTopics topic = createOffsetFetchTopics(EXISTING_TOPIC_NAME, apiVersion(), clusterFixture, 0, 20);
            for (String groupId : ALL_GROUP_IDS) {
                OffsetFetchRequestData.OffsetFetchRequestGroup group = new OffsetFetchRequestData.OffsetFetchRequestGroup();
                group.setGroupId(groupId);
                if (allTopics) {
                    group.setTopics(null);
                }
                else {
                    group.topics().add(topic);
                }
                fetchRequestData.groups().add(group);
            }
            return fetchRequestData;
        }
    }

    private static OffsetFetchRequestTopic createOffsetFetchTopic(String topicName, int... partitions) {
        OffsetFetchRequestTopic fetchTopic = new OffsetFetchRequestTopic();
        fetchTopic.setName(topicName);
        fetchTopic.setPartitionIndexes(Arrays.stream(partitions).boxed().toList());
        return fetchTopic;
    }

    private static OffsetFetchRequestTopics createOffsetFetchTopics(String topicName, short apiVersion, BaseClusterFixture clusterFixture, int... partitions) {
        OffsetFetchRequestTopics fetchTopic = new OffsetFetchRequestTopics();
        if (apiVersion < 10) {
            fetchTopic.setName(topicName);
        }
        else {
            Uuid topicId = clusterFixture.topicIds().get(topicName);
            if (topicId == null) {
                throw new IllegalStateException("no topicId available for " + topicName);
            }
            fetchTopic.setTopicId(topicId);
        }
        fetchTopic.setPartitionIndexes(Arrays.stream(partitions).boxed().toList());
        return fetchTopic;
    }
}
