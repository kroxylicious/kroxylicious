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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseDataJsonConverter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.test.Request;
import io.kroxylicious.test.record.RecordTestUtils;
import io.kroxylicious.testing.kafka.api.KafkaCluster;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

public class ProduceAuthzIT extends AuthzIT {

    private static String topicName = "topic";
    private Path rulesFile;

    private List<AclBinding> aclBindings;

    @BeforeAll
    void beforeAll() throws IOException {
        // TODO need to add Carol who has Cluster.CREATE
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                version 1;
                import User from io.kroxylicious.proxy.authentication;
                import TopicResource as Topic from io.kroxylicious.filter.authorization;
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to WRITE Topic with name = "%s";
                otherwise deny;
                """.formatted(topicName, topicName));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.WRITE, AclPermissionType.ALLOW)));
    }

    @AfterAll
    void afterAll() throws IOException {
        Files.deleteIfExists(rulesFile);
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthz, topicName, aclBindings);
        this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthz, topicName, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthz, List.of(topicName), aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthz, List.of(topicName), List.of());
    }

    class ProduceEquivalence extends Equivalence<ProduceRequestData, ProduceResponseData> {

        private final RequestTemplate<ProduceRequestData> requestTemplate;

        ProduceEquivalence(
                           short apiVersion,
                           RequestTemplate<ProduceRequestData> requestTemplate) {
            super(apiVersion);
            this.requestTemplate = requestTemplate;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "apiVersion=" + apiVersion() +
                    ", requestTemplate=" + requestTemplate +
                    '}';
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.PRODUCE;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public ProduceRequestData requestData(String user, Map<String, Uuid> topicNameToId) {
            return requestTemplate.request(user, topicNameToId);
        }

        @Override
        public ObjectNode convertResponse(ProduceResponseData response) {
            return (ObjectNode) ProduceResponseDataJsonConverter.write(response, apiVersion());
        }

        @Override
        public Map<String, Request> requests(Map<String, Uuid> topicsIds) {
            return Map.of(
                    ALICE, newRequest(requestData(ALICE, topicsIds)),
                    BOB, newRequest(requestData(BOB, topicsIds)),
                    EVE, newRequest(requestData(EVE, topicsIds)));
        }

        @Override
        public String clobberResponse(ObjectNode jsonNodes) {
            return prettyJsonString(jsonNodes);
        }

        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
            assertThat(topicContents(cluster.backingCluster()))
                    .isEqualTo(Map.of(
                            "alice", List.of("Alice"),
                            "bob", List.of("Bob")));
        }

        private Map<String, List<String>> topicContents(KafkaCluster unproxiedCluster) {
            var recordValuesGroupedByKey = new HashMap<String, List<String>>();
            try (var consumer = new KafkaConsumer<>(unproxiedCluster.getKafkaClientConfiguration(SUPER, "Super"),
                    new StringDeserializer(), new StringDeserializer())) {
                var tp = new TopicPartition(topicName, 0);
                consumer.assign(List.of(tp));
                consumer.seek(tp, 0);
                var records = consumer.poll(Duration.ofSeconds(5));
                var grouped = records.records(tp).stream()
                        .collect(Collectors.groupingBy(ConsumerRecord::key))
                        .entrySet().stream().collect(
                                Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream().map(ConsumerRecord::value).toList()));
                recordValuesGroupedByKey.putAll(grouped);
                var end = consumer.endOffsets(List.of(tp)).get(tp);
                assertThat(end).isEqualTo(2);
            }
            return recordValuesGroupedByKey;
        }

        @Override
        public void assertUnproxiedResponses(Map<String, ProduceResponseData> unproxiedResponsesByUser) {
            // TODO
        }
    }

    static long pid = 1L;

    @NonNull
    private static List<ProduceRequestData.PartitionProduceData> partitionData(String key, String value) {
        // It's important to use different pid different client instances, else ProduceReequests will get fenced out
        long producerId = pid++;
        var mr = RecordTestUtils.memoryRecords(RecordTestUtils.singleElementRecordBatch(
                RecordTestUtils.DEFAULT_MAGIC_VALUE,
                RecordTestUtils.DEFAULT_OFFSET,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                156543L, // logAppendTime
                producerId, // producerId
                (short) 0, // producerEpoch
                4, // baseSequence
                false, // isTransactional
                false, // isControlBatch
                0, // partitionLeaderEpoch
                key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)));
        assertThat(mr.firstBatchSize()).isGreaterThan(0);
        assertThat(mr.batches().iterator().next().iterator().hasNext()).isTrue();
        return List.of(new ProduceRequestData.PartitionProduceData()
                .setIndex(0)
                .setRecords(mr));
    }

    List<Arguments> test() {
        // The tuples
        List<Short> apiVersions = ApiKeys.PRODUCE.allVersions();
        String[] transactionalIds = { null, "my-txnl-id" };

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            if (apiVersion >= 13) {
                result.add(Arguments.of(new UnsupportedApiVersion<>(ApiKeys.PRODUCE, apiVersion)));
                continue;
            }
            for (String transactionalId : transactionalIds) {
                result.add(
                        Arguments.of(new ProduceEquivalence(apiVersion,
                                (user, topicIds) -> {
                                    ProduceRequestData data = new ProduceRequestData()
                                            .setTransactionalId(transactionalId)
                                            .setTimeoutMs(10_000)
                                            .setAcks((short) 1);
                                    var topicCollection = new ProduceRequestData.TopicProduceDataCollection();
                                    var t = new ProduceRequestData.TopicProduceData()
                                            .setPartitionData(partitionData(user, PASSWORDS.get(user)));
                                    // if (apiVersion >= 13) {
                                    // t.setTopicId(topicIds.get(topicName));
                                    // }
                                    // else {
                                    t.setName(topicName);
                                    // }
                                    topicCollection.mustAdd(t);
                                    data.setTopicData(topicCollection);
                                    return data;
                                })));
            }
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource
    void test(VersionSpecificVerification<ProduceRequestData, ProduceResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
