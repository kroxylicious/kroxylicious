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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.authorizer.provider.acl.AclAuthorizerService;
import io.kroxylicious.filter.authorization.Authorization;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.SaslPlainTermination;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.record.RecordTestUtils;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
public class ProduceAuthzEquivalenceIT extends AbstractAuthzEquivalenceIT {

    private static final Uuid SENTINEL_TOPIC_ID = Uuid.randomUuid();

    private static Path rulesFile;
    private static String topicName = "topic";
    private static List<AclBinding> aclBindings;
    private Uuid topicIdInUnproxiedCluster;
    private Uuid topicIdInProxiedCluster;

    public static final String SUPER = "super";
    public static final String ALICE = "alice";
    public static final String BOB = "bob";
    public static final String EVE = "eve";
    @SaslMechanism(principals = {
            @SaslMechanism.Principal(user = SUPER, password = "Super"),
            @SaslMechanism.Principal(user = ALICE, password = "Alice"),
            @SaslMechanism.Principal(user = BOB, password = "Bob"),
            @SaslMechanism.Principal(user = EVE, password = "Eve")
    }) @BrokerConfig(name = "authorizer.class.name", value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
    // ANONYMOUS is the broker
    @BrokerConfig(name = "super.users", value = "User:ANONYMOUS;User:super")
    static KafkaCluster unproxiedCluster;
    static KafkaCluster proxiedCluster;

    Map<String, String> passwords = Map.of(
            ALICE, "Alice",
            BOB, "Bob",
            EVE, "Eve");

    @BeforeAll
    static void beforeAll() throws IOException {
        // TODO need to add Carol who has Cluster.CREATE
        rulesFile = Files.createTempFile(ProduceAuthzEquivalenceIT.class.getName(), ".aclRules");
        Files.writeString(rulesFile, """
                version 1;
                import User from io.kroxylicious.proxy.internal.subject; // TODO This can't remain in the internal package!
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
    static void afterAll() throws IOException {
        Files.deleteIfExists(rulesFile);
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdInUnproxiedCluster = prepCluster(unproxiedCluster, topicName, aclBindings);
        this.topicIdInProxiedCluster = prepCluster(proxiedCluster, topicName, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(unproxiedCluster, List.of(topicName), aclBindings);
        deleteTopicsAndAcls(proxiedCluster, List.of(topicName), List.of());
    }

    private Map<String, ObjectNode> responsesByUser(short apiVersion,
                                                           String transactionalId,
                                                           List<ProduceRequestData.TopicProduceData> topics,
                                                           String bootstrapServers,
                                                           Uuid topicIdReplacement) {
        var responsesByUser = new HashMap<String, ObjectNode>();
        for (var entry : passwords.entrySet()) {
            try (KafkaClient client = client(bootstrapServers)) {
                String user = entry.getKey();
                String password = entry.getValue();
                authenticate(client, user, password);

                var b = new ProduceRequestData.TopicProduceDataCollection();
                for (var topic : topics) {
                    List<ProduceRequestData.PartitionProduceData> v = partitionData(user, password);
                    var t = new ProduceRequestData.TopicProduceData()
                            .setName(topic.name())
                            .setPartitionData(v);
                    b.mustAdd(t);
                }
                ProduceRequestData message = new ProduceRequestData()
                        .setAcks((short) 1)
                        .setTopicData(b)
                        .setTransactionalId(transactionalId)
                        .setTimeoutMs(10_000);
                var resp = client.getSync(new Request(ApiKeys.PRODUCE, apiVersion, "test",
                                message));

                var r = (ProduceResponseData) resp.payload().message();
                ObjectNode json = (ObjectNode) ProduceResponseDataJsonConverter.write(r, apiVersion);
                responsesByUser.put(user, json);
            }
        }
        return responsesByUser;
    }

    static List<Arguments> produce() {
        // The tuples
        List<Short> apiVersions = ApiKeys.PRODUCE.allVersions();
        String[] transactionalIds = { null, "my-txnl-id" };
        List[] requestedTopics = {
                List.of(
                        new ProduceRequestData.TopicProduceData()
                                .setName(topicName)
                )
        };

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            for (String transactionalId : transactionalIds) {
                for (List<ProduceRequestData.TopicProduceData> topics : requestedTopics) {
                    result.add(
                            Arguments.of(apiVersion, transactionalId, topics));
                }
            }
        }
        return result;
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

    @ParameterizedTest
    @MethodSource
    void produce(
            short apiVersion,
            @Nullable String transactionalId,
            List<ProduceRequestData.TopicProduceData> topics) {

        var unproxiedResponsesByUser = responsesByUser(apiVersion,
                transactionalId,
                duplicateList(topics),
                unproxiedCluster.getBootstrapServers(),
                topicIdInUnproxiedCluster);
        // assertions about responses
        if (topics == null || !topics.isEmpty()) {
            // Sanity test what we expect the Kafka reponse to look like
            JsonPointer errorPtr = JsonPointer.compile("/responses/0/partitionResponses/0/errorCode");
            assertErrorCodeAtPointer(ALICE, unproxiedResponsesByUser.get(ALICE), errorPtr, Errors.NONE);
            assertErrorCodeAtPointer(BOB, unproxiedResponsesByUser.get(BOB), errorPtr, Errors.NONE);
            assertErrorCodeAtPointer(EVE, unproxiedResponsesByUser.get(EVE), errorPtr, Errors.TOPIC_AUTHORIZATION_FAILED);
        }
        // assertions about side effects
        assertVisibleSideEffects(unproxiedCluster);

        NamedFilterDefinition saslTermination = new NamedFilterDefinitionBuilder(
                "authn",
                SaslPlainTermination.class.getName())
                .withConfig("userNameToPassword", passwords)
                .build();
        NamedFilterDefinition authorization = new NamedFilterDefinitionBuilder(
                "authz",
                Authorization.class.getName())
                .withConfig("authorizer", AclAuthorizerService.class.getName(),
                        "authorizerConfig", Map.of("aclFile", rulesFile.toFile().getAbsolutePath()))
                .build();
        var config = proxy(proxiedCluster)
                .addToFilterDefinitions(saslTermination, authorization)
                .addToDefaultFilters(saslTermination.name(), authorization.name());

        try (var tester = kroxyliciousTester(config)) {

            var proxiedResponsesByUser = responsesByUser(
                    apiVersion,
                    transactionalId,
                    duplicateList(topics),
                    tester.getBootstrapAddress(),
                    topicIdInProxiedCluster);

            // assert the responses from the proxied cluster at the same as from the unproxied cluster
            // (modulo clobbbering things like UUIDs which will be unavoidably different)
            assertThat(mapValues(proxiedResponsesByUser,
                    AbstractAuthzEquivalenceIT::prettyJsonString))
                    .as("Expect equivalent response to an unproxied Kafka cluster with the equivalent AuthZ")
                    .isEqualTo(mapValues(unproxiedResponsesByUser,
                            AbstractAuthzEquivalenceIT::prettyJsonString));
            // assertions about side effects
            assertVisibleSideEffects(proxiedCluster);
        }
    }

    private void assertVisibleSideEffects(KafkaCluster cluster) {
        assertThat(topicContents(cluster))
                .isEqualTo(Map.of(
                        "alice", List.of("Alice"),
                        "bob", List.of("Bob")
                ));
    }

    private Map<String, List<String>> topicContents(KafkaCluster unproxiedCluster) {
        var recordValuesGroupedByKey = new HashMap<String, List<String>>();
        try (var consumer = new KafkaConsumer<>(unproxiedCluster.getKafkaClientConfiguration(SUPER, "Super"),
                new StringDeserializer(), new StringDeserializer()) ) {
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

}