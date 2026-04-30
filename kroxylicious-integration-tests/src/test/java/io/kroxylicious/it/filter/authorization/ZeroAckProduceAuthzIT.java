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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.authorizer.provider.acl.AclAuthorizerService;
import io.kroxylicious.filter.authorization.Authorization;
import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.it.testplugins.SaslPlainTermination;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.testing.filter.record.RecordTestUtils;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.ResponsePayload;
import io.kroxylicious.testing.integration.client.KafkaClient;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.integration.tester.MockServerKroxyliciousTester;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.it.filter.authorization.AuthzIT.getRequest;
import static io.kroxylicious.proxy.internal.TopicNameRetriever.METADATA_API_VER_WITH_TOPIC_ID_SUPPORT;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.testing.integration.tester.MockServerKroxyliciousTester.zeroAckProduceRequestMatcher;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.kafka.common.protocol.ApiKeys.METADATA;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.apache.kafka.common.protocol.ApiKeys.SASL_AUTHENTICATE;
import static org.apache.kafka.common.protocol.ApiKeys.SASL_HANDSHAKE;
import static org.assertj.core.api.Assertions.assertThat;

class ZeroAckProduceAuthzIT {
    private static final String TOPIC_NAME_A = "topica";
    private static final Uuid TOPIC_ID_A = Uuid.randomUuid();
    private static final String TOPIC_NAME_B = "topicb";
    private static final Uuid TOPIC_ID_B = Uuid.randomUuid();
    public static final Map<Uuid, String> TOPIC_ID_TO_NAME = Map.of(TOPIC_ID_A, TOPIC_NAME_A, TOPIC_ID_B, TOPIC_NAME_B);
    public static final Map<String, Uuid> TOPIC_NAME_TO_ID = TOPIC_ID_TO_NAME.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    public static final String ALICE = "alice";
    public static final String BOB = "bob";
    public static final String EVE = "eve";

    public static final Map<String, String> PASSWORDS = Map.of(
            ALICE, "Alice",
            BOB, "Bob",
            EVE, "Eve");

    private static Path rulesFile;

    @BeforeAll
    static void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(ZeroAckProduceAuthzIT.class.getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to WRITE Topic with name = "%s";
                otherwise deny;
                """.formatted(TOPIC_NAME_A, TOPIC_NAME_A));
    }

    protected static ConfigurationBuilder proxyConfig(String bootstrap,
                                                      Map<String, String> passwords,
                                                      Path rulesFile) {
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
        return proxy(bootstrap)
                .addToFilterDefinitions(saslTermination, authorization)
                .addToDefaultFilters(saslTermination.name(), authorization.name())
                .editMatchingVirtualCluster(x -> true)
                .endVirtualCluster();
    }

    @ParameterizedTest
    @MethodSource
    void testZeroAckProduceAuthz(short apiVersion, Expectation expectation) throws Exception {
        try (MockServerKroxyliciousTester mockServerKroxyliciousTester = KroxyliciousTesters.mockKafkaKroxyliciousTester(
                bootstrap -> proxyConfig(bootstrap, PASSWORDS, rulesFile));
                KafkaClient kafkaClient = mockServerKroxyliciousTester.simpleTestClient()) {
            givenMockSaslServer(mockServerKroxyliciousTester, TOPIC_ID_TO_NAME);
            AuthzIT.authenticate(kafkaClient, expectation.user, PASSWORDS.get(expectation.user));
            ProduceRequestData produceRequestData = getProduceRequestData(expectation.topicsInProduce(), apiVersion);
            mockServerKroxyliciousTester.dropWhen(zeroAckProduceRequestMatcher());
            kafkaClient.get(new Request(PRODUCE, apiVersion, "client", produceRequestData)).get(5, SECONDS);
            // send another arbitrary message as a way of ensuring the produce has completed its fire and forget send on the same channel
            sendArbitraryMessageExpectedResponse(kafkaClient);
            if (expectation.topicsForwarded().isEmpty()) {
                assertThat(mockServerKroxyliciousTester.getRequestsForApiKey(PRODUCE)).isEmpty();
            }
            else {
                Awaitility.await().untilAsserted(() -> {
                    assertThat(mockServerKroxyliciousTester.getRequestsForApiKey(PRODUCE)).hasSize(1)
                            .singleElement().satisfies(request -> {
                                ProduceRequestData message = (ProduceRequestData) request.message();
                                Set<String> forwardedTopics = message.topicData().stream().map(ZeroAckProduceAuthzIT::getName).collect(toSet());
                                assertThat(forwardedTopics).containsExactlyInAnyOrderElementsOf(expectation.topicsForwarded());
                            });
                });
            }
        }
    }

    private static String getName(ProduceRequestData.TopicProduceData topicProduceData) {
        return Optional.ofNullable(topicProduceData.name())
                .filter(s -> !s.isEmpty())
                .or(() -> Optional.ofNullable(TOPIC_ID_TO_NAME.get(topicProduceData.topicId())))
                .orElseThrow();
    }

    private static void sendArbitraryMessageExpectedResponse(KafkaClient kafkaClient) {
        var handshakeResponse = (SaslHandshakeResponseData) kafkaClient.getSync(getRequest(ApiKeys.SASL_HANDSHAKE.latestVersion(),
                new SaslHandshakeRequestData().setMechanism("PLAIN")))
                .payload().message();
        assertThat(Errors.forCode(handshakeResponse.errorCode())).isEqualTo(Errors.NONE);
    }

    private static void givenMockSaslServer(MockServerKroxyliciousTester mockServerKroxyliciousTester, Map<Uuid, String> topicNames) {
        SaslHandshakeResponseData handshakeResponse = new SaslHandshakeResponseData();
        handshakeResponse.setMechanisms(List.of(PlainSaslServer.PLAIN_MECHANISM));
        ResponsePayload handshakePayload = new ResponsePayload(SASL_HANDSHAKE, SASL_HANDSHAKE.latestVersion(), handshakeResponse);
        mockServerKroxyliciousTester.addMockResponseForApiKey(handshakePayload);
        SaslAuthenticateResponseData authResponse = new SaslAuthenticateResponseData();
        authResponse.setAuthBytes(new byte[0]);
        authResponse.setErrorCode(Errors.NONE.code());
        ResponsePayload authPayload = new ResponsePayload(SASL_AUTHENTICATE, SASL_AUTHENTICATE.latestVersion(), handshakeResponse);
        mockServerKroxyliciousTester.addMockResponseForApiKey(authPayload);
        MetadataResponseData metadataResponseData = new MetadataResponseData();
        topicNames.forEach((key, value) -> {
            MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic();
            topic.setName(value).setTopicId(key);
            metadataResponseData.topics().add(topic);
        });
        mockServerKroxyliciousTester.addMockResponseForApiKey(new ResponsePayload(METADATA, METADATA_API_VER_WITH_TOPIC_ID_SUPPORT, metadataResponseData));
    }

    record Expectation(String user, List<String> topicsInProduce, List<String> topicsForwarded) {
        @NonNull
        @Override
        public String toString() {
            String forwarded;
            if (topicsForwarded.isEmpty()) {
                forwarded = "produce to be dropped";
            }
            else {
                forwarded = topicsForwarded + " to be forwarded";
            }
            return "user " + user + " sends topics " + topicsInProduce().toString() + " expecting " + forwarded;
        }
    }

    static Stream<Arguments> testZeroAckProduceAuthz() {
        IntStream apiVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(PRODUCE), AuthorizationFilter.maxSupportedApiVersion(PRODUCE));
        // alice and bob are allowed to access A, eve is not allowed any access
        List<String> bothTopics = List.of(TOPIC_NAME_A, TOPIC_NAME_B);
        List<String> aOnly = List.of(TOPIC_NAME_A);
        // nobody is allowed to access B
        List<String> bOnly = List.of(TOPIC_NAME_B);
        List<String> nothingForwarded = List.of();
        List<Expectation> expectations = List.of(
                new Expectation(ALICE, bothTopics, aOnly),
                new Expectation(ALICE, aOnly, aOnly),
                new Expectation(ALICE, bOnly, nothingForwarded),
                new Expectation(BOB, bothTopics, aOnly),
                new Expectation(BOB, aOnly, aOnly),
                new Expectation(BOB, bOnly, nothingForwarded),
                new Expectation(EVE, bothTopics, nothingForwarded),
                new Expectation(EVE, aOnly, nothingForwarded),
                new Expectation(EVE, bOnly, nothingForwarded));
        return apiVersions.boxed().flatMap(
                apiVersion -> expectations.stream()
                        .map(expectation -> Arguments.argumentSet("api version " + apiVersion + " " + expectation, (short) (int) apiVersion, expectation)));
    }

    private static ProduceRequestData getProduceRequestData(List<String> topicsToSendDataTo, short apiVersion) {
        ProduceRequestData data = new ProduceRequestData()
                .setTimeoutMs(10_000)
                .setAcks((short) 0);
        var topicCollection = new ProduceRequestData.TopicProduceDataCollection();
        for (String topic : topicsToSendDataTo) {
            ProduceRequestData.TopicProduceData topicProduceData = new ProduceRequestData.TopicProduceData()
                    .setPartitionData(partitionData());
            ProduceRequestData.TopicProduceData t;
            if (apiVersion >= 13) {
                t = topicProduceData.setTopicId(TOPIC_NAME_TO_ID.get(topic));
            }
            else {
                t = topicProduceData.setName(topic);
            }
            topicCollection.mustAdd(t);
        }
        data.setTopicData(topicCollection);
        return data;
    }

    private static List<ProduceRequestData.PartitionProduceData> partitionData() {
        long currentTimeMillis = System.currentTimeMillis();
        var mr = RecordTestUtils.memoryRecords(RecordTestUtils.singleElementRecordBatch(
                RecordTestUtils.DEFAULT_MAGIC_VALUE,
                RecordTestUtils.DEFAULT_OFFSET,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                currentTimeMillis, // logAppendTime
                1, // producerId
                (short) 0, // producerEpoch
                4, // baseSequence
                false, // isTransactional
                false, // isControlBatch
                0, // partitionLeaderEpoch
                "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8), currentTimeMillis));
        assertThat(mr.firstBatchSize()).isGreaterThan(0);
        assertThat(mr.batches().iterator().next().iterator().hasNext()).isTrue();
        return List.of(new ProduceRequestData.PartitionProduceData()
                .setIndex(0)
                .setRecords(mr));
    }

}
