/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
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
import io.kroxylicious.testing.kafka.junit5ext.Name;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class ProduceAuthzTxnlIdIT extends AuthzIT {

    private String topicName;
    private static final String ALICE_TRANSACTIONAL_ID_PREFIX = "alice-transaction";
    private static final String BOB_TRANSACTIONAL_ID_PREFIX = "bob-transaction";
    private static final List<String> ALL_TRANSACTIONAL_ID_PREFIXES = List.of(ALICE_TRANSACTIONAL_ID_PREFIX, BOB_TRANSACTIONAL_ID_PREFIX);
    private Path rulesFile;

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TransactionalIdResource as TxnlId;
                allow User with name = "alice" to * TxnlId with name like "%s*";
                allow User with name = "bob" to WRITE TxnlId with name like "%s*";
                allow User with name = "super" to * TxnlId with name like "*";
                otherwise deny;
                """.formatted(ALICE_TRANSACTIONAL_ID_PREFIX, BOB_TRANSACTIONAL_ID_PREFIX));
    }

    private List<AclBinding> aclBindings() {
        return List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, getTopicName(), PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, getTopicName(), PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, ALICE_TRANSACTIONAL_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, BOB_TRANSACTIONAL_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.WRITE, AclPermissionType.ALLOW)));
    }

    @AfterAll
    void afterAll() throws IOException {
        Files.deleteIfExists(rulesFile);
    }

    @BeforeEach
    void prepClusters() {
        // prevent any chance of races around deletion/recreation of topics
        topicName = "topic-" + UUID.randomUUID();
        this.topicIdsInUnproxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(getTopicName()), aclBindings());
        this.topicIdsInProxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(getTopicName()), List.of());
    }

    @AfterEach
    void tidyClusters() {
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(getTopicName()), aclBindings());
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(getTopicName()), List.of());
    }

    class ProduceEquivalence extends Equivalence<ProduceRequestData, ProduceResponseData> {

        private final String transactionalId;
        private ProducerIdAndEpoch producerIdAndEpoch;

        ProduceEquivalence(
                           short apiVersion,
                           String transactionalId) {
            super(apiVersion);
            this.transactionalId = transactionalId;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "apiVersion=" + apiVersion() +
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
        public ProduceRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            ProduceRequestData data = new ProduceRequestData()
                    .setTransactionalId(transactionalId)
                    .setTimeoutMs(10_000)
                    .setAcks((short) 1);
            var topicCollection = new ProduceRequestData.TopicProduceDataCollection();
            var t = new ProduceRequestData.TopicProduceData()
                    .setPartitionData(partitionData(user, PASSWORDS.get(user), Objects.requireNonNull(producerIdAndEpoch)));
            t.setName(getTopicName());
            topicCollection.mustAdd(t);
            data.setTopicData(topicCollection);
            return data;
        }

        @Override
        public Map<String, Request> requests(BaseClusterFixture clusterFixture) {
            return Map.of(
                    ALICE, newRequest(requestData(ALICE, clusterFixture)),
                    BOB, newRequest(requestData(BOB, clusterFixture)),
                    EVE, newRequest(requestData(EVE, clusterFixture)));
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            return prettyJsonString(jsonNodes);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
            Map<String, List<String>> expectation;
            if (transactionalId.startsWith(ALICE_TRANSACTIONAL_ID_PREFIX)) {
                expectation = Map.of(ALICE, List.of(PASSWORDS.get(ALICE)));
            }
            else if (transactionalId.startsWith(BOB_TRANSACTIONAL_ID_PREFIX)) {
                expectation = Map.of(BOB, List.of(PASSWORDS.get(BOB)));
            }
            else {
                throw new IllegalStateException("unexpected transactionalId " + transactionalId);
            }
            assertThat(topicContents(cluster.backingCluster(), 1))
                    .isEqualTo(expectation);
        }

        private Map<String, List<String>> topicContents(KafkaCluster unproxiedCluster, int expectedRecords) {
            var recordValuesGroupedByKey = new HashMap<String, List<String>>();
            Map<String, Object> consumerConfig = unproxiedCluster.getKafkaClientConfiguration(SUPER, "Super");
            consumerConfig.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 0);
            consumerConfig.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100);
            consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString());
            try (var consumer = new KafkaConsumer<>(consumerConfig,
                    new StringDeserializer(), new StringDeserializer())) {
                var tp = new TopicPartition(getTopicName(), 0);
                Long endOffset = consumer.endOffsets(List.of(tp)).values().stream().findFirst().orElseThrow();
                assertThat(endOffset).isEqualTo(expectedRecords);
                consumer.assign(List.of(tp));
                consumer.seek(tp, 0);
                long consumed = 0;
                long start = System.currentTimeMillis();
                while (consumed < expectedRecords && System.currentTimeMillis() - start < 5000) {
                    var records = consumer.poll(Duration.ofSeconds(0));
                    records.records(tp)
                            .forEach(record -> recordValuesGroupedByKey.computeIfAbsent(record.key(), k -> new ArrayList<>())
                                    .add(record.value()));
                    consumed += records.count();
                }
            }
            return recordValuesGroupedByKey;
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            KafkaDriver driver = new KafkaDriver(cluster, cluster.authenticatedClient(AuthzIT.SUPER, SUPER_PASSWORD), AuthzIT.SUPER);
            driver.findCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
            producerIdAndEpoch = driver.initProducerId(transactionalId);
            driver.addPartitionsToTransaction(transactionalId, producerIdAndEpoch, Map.of(getTopicName(), Set.of(0)));
        }
    }

    private String getTopicName() {
        return Objects.requireNonNull(topicName);
    }

    @NonNull
    private static List<ProduceRequestData.PartitionProduceData> partitionData(String key, String value, ProducerIdAndEpoch idAndEpoch) {
        var mr = RecordTestUtils.memoryRecords(RecordTestUtils.singleElementRecordBatch(
                RecordTestUtils.DEFAULT_MAGIC_VALUE,
                RecordTestUtils.DEFAULT_OFFSET,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                156543L, // logAppendTime
                idAndEpoch.producerId, // producerId
                idAndEpoch.epoch, // producerEpoch
                0, // baseSequence
                true, // isTransactional
                false, // isControlBatch
                0, // partitionLeaderEpoch
                key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)));
        assertThat(mr.firstBatchSize()).isGreaterThan(0);
        assertThat(mr.batches().iterator().next().iterator().hasNext()).isTrue();
        return List.of(new ProduceRequestData.PartitionProduceData()
                .setIndex(0)
                .setRecords(mr));
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        // The tuples
        List<Short> apiVersions = ApiKeys.PRODUCE.allVersions();

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            if (apiVersion >= 13) {
                result.add(Arguments.argumentSet("unsupported version " + apiVersion, new UnsupportedApiVersion<>(ApiKeys.PRODUCE, apiVersion)));
                continue;
            }
            for (String transactionalIdPrefix : ALL_TRANSACTIONAL_ID_PREFIXES) {
                String transactionalId = transactionalIdPrefix + "-" + UUID.randomUUID();
                result.add(
                        Arguments.argumentSet("apiVersion " + apiVersion + " transactionalIdPrefix " + transactionalIdPrefix,
                                new ProduceEquivalence(apiVersion, transactionalId)));
            }
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<ProduceRequestData, ProduceResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
