/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.filter.ForwardingStyle;
import io.kroxylicious.proxy.filter.RejectingCreateTopicFilter;
import io.kroxylicious.proxy.filter.RejectingCreateTopicFilterFactory;
import io.kroxylicious.proxy.filter.RequestResponseMarkingFilter;
import io.kroxylicious.proxy.filter.RequestResponseMarkingFilterFactory;
import io.kroxylicious.proxy.filter.simpletransform.FetchResponseTransformation;
import io.kroxylicious.proxy.filter.simpletransform.ProduceRequestTransformation;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.UnknownTaggedFields.unknownTaggedFieldsToStrings;
import static io.kroxylicious.proxy.filter.RequestResponseMarkingFilter.FILTER_NAME_TAG;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static io.kroxylicious.test.tester.MockServerKroxyliciousTester.zeroAckProduceRequestMatcher;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.CREATE_TOPICS;
import static org.apache.kafka.common.protocol.ApiKeys.FETCH;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_GROUPS;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_TRANSACTIONS;
import static org.apache.kafka.common.protocol.ApiKeys.METADATA;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class FilterIT {

    private static final String PLAINTEXT = "Hello, world!";
    private static final NamedFilterDefinitionBuilder REJECTING_CREATE_TOPIC_FILTER = new NamedFilterDefinitionBuilder(RejectingCreateTopicFilterFactory.class.getName(),
            RejectingCreateTopicFilterFactory.class.getName());
    public static final String SCRAM_256 = "SCRAM-SHA-256";
    public static final String SCRAM_512 = "SCRAM-SHA-512";
    public static final String SCRAM_USER = "user";
    public static final String SCRAM_PAZZ = "pazz";

    @Test
    void reversibleEncryption() {
        // The precise details of the cipher don't matter
        // What matters is that it the ciphertext key depends on the topic name
        // and that decode() is the inverse of encode()
        var name = UUID.randomUUID().toString();
        var encoded = encode(name, ByteBuffer.wrap(PLAINTEXT.getBytes(StandardCharsets.UTF_8)));
        var decoded = new String(decode(name, encoded).array(), StandardCharsets.UTF_8);
        assertThat(decoded)
                .isEqualTo(PLAINTEXT);
    }

    @Test
    void encryptionDistinguishedByName() {
        var name = UUID.randomUUID().toString();
        var differentName = UUID.randomUUID().toString();
        assertThat(differentName).isNotEqualTo(name);

        var encoded = encode(name, ByteBuffer.wrap(PLAINTEXT.getBytes(StandardCharsets.UTF_8)));

        assertThat(encoded)
                .isNotEqualTo(encode(differentName, ByteBuffer.wrap(PLAINTEXT.getBytes(StandardCharsets.UTF_8))));
    }

    static ByteBuffer encode(String topicName, ByteBuffer in) {
        var out = ByteBuffer.allocate(in.limit());
        for (int index = 0; index < in.limit(); index++) {
            byte rot = getRot(topicName, index);
            byte b = in.get(index);
            byte rotated = (byte) (b + rot);
            out.put(index, rotated);
        }
        return out;
    }

    private static byte getRot(String topicName, int index) {
        char c = topicName.charAt(index % topicName.length());
        int i = topicName.hashCode() + c;
        return (byte) (i % Byte.MAX_VALUE);
    }

    static ByteBuffer decode(String topicName, ByteBuffer in) {
        var out = ByteBuffer.allocate(in.limit());
        out.limit(in.limit());
        for (int index = 0; index < in.limit(); index++) {
            byte b = in.get(index);
            byte rot = (byte) -getRot(topicName, index);
            byte rotated = (byte) (b + rot);
            out.put(index, rotated);
        }
        return out;
    }

    @Test
    void shouldPassThroughRecordUnchanged(KafkaCluster cluster, Topic topic) throws Exception {

        try (var tester = kroxyliciousTester(proxy(cluster));
                var producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "shouldPassThroughRecordUnchanged", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester.consumer()) {
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "Hello, world!")).get();
            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));
            consumer.close();

            assertThat(records.iterator())
                    .toIterable()
                    .hasSize(1)
                    .map(ConsumerRecord::value)
                    .containsExactly(PLAINTEXT);

        }
    }

    @Test
    void scramSha256Test(@SaslMechanism(value = SCRAM_256, principals = @SaslMechanism.Principal(user = SCRAM_USER, password = SCRAM_PAZZ)) KafkaCluster cluster,
                         Topic topic)
            throws Exception {

        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put(CLIENT_ID_CONFIG, "shouldPassThroughRecordUnchanged");
        clientConfig.put(DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000);
        clientConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
                String.format("""
                        %s required username="%s" password="%s";""",
                        ScramLoginModule.class.getName(), SCRAM_USER, SCRAM_PAZZ));
        clientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        clientConfig.put(SaslConfigs.SASL_MECHANISM, SCRAM_256);

        Map<String, Object> consumerConfig = new HashMap<>(clientConfig);
        consumerConfig.put(GROUP_ID_CONFIG, "group");
        consumerConfig.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var tester = kroxyliciousTester(proxy(cluster));
                var producer = tester.producer(clientConfig);
                var consumer = tester.consumer(consumerConfig)) {
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "Hello, world!")).get();
            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));
            consumer.close();

            assertThat(records.iterator())
                    .toIterable()
                    .hasSize(1)
                    .map(ConsumerRecord::value)
                    .containsExactly(PLAINTEXT);

        }
    }

    @Test
    void scramSha512Test(@SaslMechanism(value = SCRAM_512, principals = @SaslMechanism.Principal(user = SCRAM_USER, password = SCRAM_PAZZ)) KafkaCluster cluster,
                         Topic topic)
            throws Exception {

        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put(CLIENT_ID_CONFIG, "shouldPassThroughRecordUnchanged");
        clientConfig.put(DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000);
        clientConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
                String.format("""
                        %s required username="%s" password="%s";""",
                        ScramLoginModule.class.getName(), SCRAM_USER, SCRAM_PAZZ));
        clientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        clientConfig.put(SaslConfigs.SASL_MECHANISM, SCRAM_512);

        Map<String, Object> consumerConfig = new HashMap<>(clientConfig);
        consumerConfig.put(GROUP_ID_CONFIG, "group");
        consumerConfig.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var tester = kroxyliciousTester(proxy(cluster));
                var producer = tester.producer(clientConfig);
                var consumer = tester.consumer(consumerConfig)) {
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "Hello, world!")).get();
            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));
            consumer.close();

            assertThat(records.iterator())
                    .toIterable()
                    .hasSize(1)
                    .map(ConsumerRecord::value)
                    .containsExactly(PLAINTEXT);

        }
    }

    @Test
    @SuppressWarnings("java:S5841")
    // java:S5841 warns that doesNotContain passes for the empty case. Which is what we want here.
    void requestFiltersCanRespondWithoutProxying(KafkaCluster cluster, Admin admin) throws Exception {
        var config = proxy(cluster)
                .addToFilterDefinitions(REJECTING_CREATE_TOPIC_FILTER.build())
                .addToDefaultFilters(REJECTING_CREATE_TOPIC_FILTER.name());

        var topicName = UUID.randomUUID().toString();
        try (var tester = kroxyliciousTester(config);
                var proxyAdmin = tester.admin()) {
            assertCreatingTopicThrowsExpectedException(proxyAdmin, topicName);

            // check no topic created on the cluster
            Set<String> names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(names).doesNotContain(topicName);
        }
    }

    static Stream<Arguments> requestFilterCanShortCircuitResponse() {
        return Stream.of(
                Arguments.of("synchronous with close", true, ForwardingStyle.SYNCHRONOUS),
                Arguments.of("synchronous without close", false, ForwardingStyle.SYNCHRONOUS),
                Arguments.of("asynchronous with close", true, ForwardingStyle.ASYNCHRONOUS_DELAYED),
                Arguments.of("asynchronous without close", true, ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void requestFilterCanShortCircuitResponse(String name, boolean withCloseConnection, ForwardingStyle forwardingStyle) {
        var rejectFilter = REJECTING_CREATE_TOPIC_FILTER
                .withConfig("withCloseConnection", withCloseConnection,
                        "forwardingStyle", forwardingStyle)
                .build();
        try (var tester = mockKafkaKroxyliciousTester((mockBootstrap) -> proxy(mockBootstrap)
                .addToFilterDefinitions(rejectFilter)
                .addToDefaultFilters(REJECTING_CREATE_TOPIC_FILTER.name()));
                var requestClient = tester.simpleTestClient()) {

            if (forwardingStyle == ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER) {
                tester.addMockResponseForApiKey(new ResponsePayload(LIST_GROUPS, LIST_GROUPS.latestVersion(), new ListGroupsResponseData()));
            }

            var createTopic = new CreateTopicsRequestData();
            createTopic.topics().add(new CreateTopicsRequestData.CreatableTopic().setName("foo"));

            var response = requestClient.getSync(new Request(CREATE_TOPICS, CREATE_TOPICS.latestVersion(), "client", createTopic));
            assertThat(response.payload().message()).isInstanceOf(CreateTopicsResponseData.class);

            var responseMessage = (CreateTopicsResponseData) response.payload().message();
            assertThat(responseMessage.topics())
                    .hasSameSizeAs(createTopic.topics())
                    .allMatch(p -> p.errorCode() == Errors.INVALID_TOPIC_EXCEPTION.code(),
                            "response contains topics without the expected errorCode");

            var expectConnectionOpen = !withCloseConnection;
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(requestClient.isOpen()).isEqualTo(expectConnectionOpen));
        }
    }

    /**
     * This test verifies the use-case where a filter needs delay a request/response forward
     * until a 3rd party asynchronous action completes.
     * @param direction direction of the flow
     */
    @ParameterizedTest
    @EnumSource(value = RequestResponseMarkingFilterFactory.Direction.class)
    void supportsForwardDeferredByAsynchronousAction(RequestResponseMarkingFilterFactory.Direction direction) {
        doSupportsForwardDeferredByAsynchronousRequest(direction,
                "supportsForwardDeferredByAsynchronousAction",
                ForwardingStyle.ASYNCHRONOUS_DELAYED);
    }

    /**
     * This test verifies the use-case where a filter needs delay a request/response forward
     * until a 3rd party asynchronous action completes.
     * @param direction direction of the flow
     */
    @ParameterizedTest
    @EnumSource(value = RequestResponseMarkingFilterFactory.Direction.class)
    void supportsForwardDeferredByAsynchronousActionOnEventLoop(RequestResponseMarkingFilterFactory.Direction direction) {
        doSupportsForwardDeferredByAsynchronousRequest(direction,
                "supportsForwardDeferredByAsynchronousActionOnEventLoop",
                ForwardingStyle.ASYNCHRONOUS_DELAYED_ON_EVENTlOOP);
    }

    /**
     * This test verifies the use-case where a filter needs delay a request/response forward
     * until an asynchronous request to the broker completes.
     * @param direction direction of the flow
     */
    @ParameterizedTest
    @EnumSource(value = RequestResponseMarkingFilterFactory.Direction.class)
    void supportsForwardDeferredByAsynchronousBrokerRequest(RequestResponseMarkingFilterFactory.Direction direction) {
        doSupportsForwardDeferredByAsynchronousRequest(direction,
                "supportsForwardDeferredByAsynchronousBrokerRequest",
                ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER);
    }

    private void doSupportsForwardDeferredByAsynchronousRequest(RequestResponseMarkingFilterFactory.Direction direction, String name,
                                                                ForwardingStyle forwardingStyle) {
        var markingFilter = new NamedFilterDefinitionBuilder(name, RequestResponseMarkingFilterFactory.class.getName())
                .withConfig("keysToMark", Set.of(LIST_TRANSACTIONS),
                        "direction", Set.of(direction),
                        "name", name,
                        "forwardingStyle", forwardingStyle)
                .build();
        try (var tester = mockKafkaKroxyliciousTester((mockBootstrap) -> proxy(mockBootstrap)
                .addToFilterDefinitions(markingFilter)
                .addToDefaultFilters(name));
                var kafkaClient = tester.simpleTestClient()) {
            tester.addMockResponseForApiKey(new ResponsePayload(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), new ListTransactionsResponseData()));
            ApiVersionsResponseData apiVersionsResponseData = new ApiVersionsResponseData();
            apiVersionsResponseData.apiKeys()
                    .add(new ApiVersionsResponseData.ApiVersion().setApiKey(FETCH.id).setMaxVersion(FETCH.latestVersion()).setMinVersion(FETCH.oldestVersion()));
            tester.addMockResponseForApiKey(new ResponsePayload(API_VERSIONS, API_VERSIONS.latestVersion(), apiVersionsResponseData));

            // In the ASYNCHRONOUS_REQUEST_TO_BROKER case, the filter will send an async list_group
            // request to the broker and defer the forward of the list transaction response until the list groups
            // response arrives.
            if (forwardingStyle == ForwardingStyle.ASYNCHRONOUS_REQUEST_TO_BROKER) {
                tester.addMockResponseForApiKey(new ResponsePayload(LIST_GROUPS, LIST_GROUPS.latestVersion(), new ListGroupsResponseData()));
            }

            var response = kafkaClient
                    .getSync(new Request(LIST_TRANSACTIONS, LIST_TRANSACTIONS.latestVersion(), "client", new ListTransactionsRequestData()));
            var requestMessageReceivedByBroker = tester.getOnlyRequestForApiKey(LIST_TRANSACTIONS).message();
            var responseMessageReceivedByClient = response.payload().message();

            assertThat(requestMessageReceivedByBroker).isInstanceOf(ListTransactionsRequestData.class);
            assertThat(responseMessageReceivedByClient).isInstanceOf(ListTransactionsResponseData.class);

            var target = direction == RequestResponseMarkingFilterFactory.Direction.REQUEST ? requestMessageReceivedByBroker : responseMessageReceivedByClient;
            assertThat(unknownTaggedFieldsToStrings(target, FILTER_NAME_TAG)).containsExactly(
                    RequestResponseMarkingFilter.class.getSimpleName() + "-%s-%s".formatted(name, direction.toString().toLowerCase(Locale.ROOT)));
        }
    }

    @Test
    @SuppressWarnings("java:S5841")
    // java:S5841 warns that doesNotContain passes for the empty case. Which is what we want here.
    void requestFiltersCanRespondWithoutProxyingDoesntLeakBuffers(KafkaCluster cluster, Admin admin) throws Exception {
        var config = proxy(cluster)
                .addToFilterDefinitions(REJECTING_CREATE_TOPIC_FILTER.build())
                .addToDefaultFilters(REJECTING_CREATE_TOPIC_FILTER.name());

        var name = UUID.randomUUID().toString();
        try (var tester = kroxyliciousTester(config);
                var proxyAdmin = tester.admin()) {
            // loop because System.gc doesn't make any guarantees that the buffer will be collected
            for (int i = 0; i < 20; i++) {
                // CreateTopicRejectFilter allocates a buffer and then short-circuit responds
                assertCreatingTopicThrowsExpectedException(proxyAdmin, name);
                // buffers must be garbage collected before it causes leak detection during
                // a subsequent buffer allocation
                System.gc();
            }

            // check no topic created on the cluster
            Set<String> names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(names).doesNotContain(name);
        }
    }

    private static void assertCreatingTopicThrowsExpectedException(Admin proxyAdmin, String topicName) {
        assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(() -> proxyAdmin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get())
                .withCauseInstanceOf(InvalidTopicException.class)
                .havingCause()
                .withMessage(RejectingCreateTopicFilter.ERROR_MESSAGE);
    }

    @Test
    void shouldModifyProduceMessage(KafkaCluster cluster, Topic topic1, Topic topic2) throws Exception {

        var bytes = PLAINTEXT.getBytes(StandardCharsets.UTF_8);
        var expectedEncoded1 = encode(topic1.name(), ByteBuffer.wrap(bytes)).array();
        var expectedEncoded2 = encode(topic2.name(), ByteBuffer.wrap(bytes)).array();

        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(ProduceRequestTransformation.class.getName(),
                ProduceRequestTransformation.class.getName())
                .withConfig("transformation", TestEncoderFactory.class.getName()).build();
        var config = proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name());

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "shouldModifyProduceMessage", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester
                        .consumer(Serdes.String(), Serdes.ByteArray(), Map.of(GROUP_ID_CONFIG, "my-group-id", AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            producer.send(new ProducerRecord<>(topic1.name(), "my-key", PLAINTEXT)).get();
            producer.send(new ProducerRecord<>(topic2.name(), "my-key", PLAINTEXT)).get();
            producer.flush();

            consumer.subscribe(Set.of(topic1.name(), topic2.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            assertThat(records).hasSize(2);
            assertThat(records.records(topic1.name()))
                    .hasSize(1)
                    .map(ConsumerRecord::value)
                    .containsExactly(expectedEncoded1);

            assertThat(records.records(topic2.name()))
                    .hasSize(1)
                    .map(ConsumerRecord::value)
                    .containsExactly(expectedEncoded2);
        }
    }

    @Test
    void requestFiltersCanRespondWithoutProxyingRespondsInCorrectOrder() throws Exception {

        try (var tester = mockKafkaKroxyliciousTester(
                s -> proxy(s).addToFilterDefinitions(REJECTING_CREATE_TOPIC_FILTER.build())
                        .addToDefaultFilters(REJECTING_CREATE_TOPIC_FILTER.name()));
                var client = tester.simpleTestClient()) {
            tester.addMockResponseForApiKey(new ResponsePayload(METADATA, METADATA.latestVersion(), new MetadataResponseData()));
            tester.addMockResponseForApiKey(new ResponsePayload(API_VERSIONS, API_VERSIONS.latestVersion(), new ApiVersionsResponseData()));
            CreateTopicsRequestData.CreatableTopic topic = new CreateTopicsRequestData.CreatableTopic();
            CreateTopicsRequestData data = new CreateTopicsRequestData();
            data.topics().add(topic);
            client.getSync(new Request(API_VERSIONS, API_VERSIONS.latestVersion(), "client", new ApiVersionsRequestData()));
            Request requestA = new Request(METADATA, METADATA.latestVersion(), "client", new MetadataRequestData());
            Request requestB = new Request(CREATE_TOPICS, CREATE_TOPICS.latestVersion(), "client", data);
            var futureA = client.get(requestA);
            var futureB = client.get(requestB);
            Response responseA = futureA.get(10, TimeUnit.SECONDS);
            Response responseB = futureB.get(10, TimeUnit.SECONDS);
            assertThat(responseA.sequenceNumber()).withFailMessage(() -> "responses received out of order").isLessThan(responseB.sequenceNumber());
        }
    }

    @Test
    void clientsCanSendMultipleMessagesImmediately() {

        try (var tester = mockKafkaKroxyliciousTester(
                s -> proxy(s).addToFilterDefinitions(REJECTING_CREATE_TOPIC_FILTER.build())
                        .addToDefaultFilters(REJECTING_CREATE_TOPIC_FILTER.name()));
                var client = tester.simpleTestClient()) {
            tester.addMockResponseForApiKey(new ResponsePayload(METADATA, METADATA.latestVersion(), new MetadataResponseData()));
            tester.addMockResponseForApiKey(new ResponsePayload(API_VERSIONS, API_VERSIONS.latestVersion(), new ApiVersionsResponseData()));
            Request requestA = new Request(METADATA, METADATA.latestVersion(), "clientA", new MetadataRequestData());
            Request requestB = new Request(API_VERSIONS, API_VERSIONS.latestVersion(), "clientB", new ApiVersionsRequestData());
            var futureA = client.get(requestA);
            var futureB = client.get(requestB);
            assertThat(futureA).succeedsWithin(10, TimeUnit.SECONDS);
            assertThat(futureB).succeedsWithin(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void zeroAckProduceRequestsDoNotInterfereWithResponseReorderingLogic() throws Exception {

        try (var tester = mockKafkaKroxyliciousTester(
                s -> proxy(s).addToFilterDefinitions(REJECTING_CREATE_TOPIC_FILTER.build())
                        .addToDefaultFilters(REJECTING_CREATE_TOPIC_FILTER.name()));
                var client = tester.simpleTestClient()) {
            tester.addMockResponseForApiKey(new ResponsePayload(METADATA, METADATA.latestVersion(), new MetadataResponseData()));
            tester.dropWhen(zeroAckProduceRequestMatcher());
            tester.addMockResponseForApiKey(new ResponsePayload(API_VERSIONS, API_VERSIONS.latestVersion(), new ApiVersionsResponseData()));
            CreateTopicsRequestData.CreatableTopic topic = new CreateTopicsRequestData.CreatableTopic();
            CreateTopicsRequestData data = new CreateTopicsRequestData();
            data.topics().add(topic);
            client.getSync(new Request(API_VERSIONS, API_VERSIONS.latestVersion(), "client", new ApiVersionsRequestData()));
            Request requestA = new Request(METADATA, METADATA.latestVersion(), "client", new MetadataRequestData());
            Request requestB = new Request(PRODUCE, PRODUCE.latestVersion(), "client", new ProduceRequestData().setAcks((short) 0));
            Request requestC = new Request(CREATE_TOPICS, CREATE_TOPICS.latestVersion(), "client", data);
            var futureA = client.get(requestA);
            client.get(requestB);
            var futureC = client.get(requestC);
            Response responseA = futureA.get(10, TimeUnit.SECONDS);
            Response responseC = futureC.get(10, TimeUnit.SECONDS);
            assertThat(responseA.sequenceNumber()).withFailMessage(() -> "responses received out of order").isLessThan(responseC.sequenceNumber());
        }
    }

    // zero-ack produce requests require special handling because they have no response associated
    // this checks that Kroxy can handle the basics of forwarding them.
    @Test
    void shouldModifyZeroAckProduceMessage(KafkaCluster cluster, Topic topic) throws Exception {
        String className = ProduceRequestTransformation.class.getName();
        var config = proxy(cluster)
                .addToFilterDefinitions(new NamedFilterDefinitionBuilder(className, className)
                        .withConfig("transformation", TestEncoderFactory.class.getName()).build())
                .addToDefaultFilters(className);

        var expectedEncoded = encode(topic.name(), ByteBuffer.wrap(PLAINTEXT.getBytes(StandardCharsets.UTF_8))).array();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "shouldModifyProduceMessage", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000, ACKS_CONFIG, "0"));
                var consumer = tester
                        .consumer(Serdes.String(), Serdes.ByteArray(), Map.of(GROUP_ID_CONFIG, "my-group-id", AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            producer.send(new ProducerRecord<>(topic.name(), "my-key", PLAINTEXT)).get();
            producer.flush();

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            assertThat(records.iterator())
                    .toIterable()
                    .hasSize(1)
                    .map(ConsumerRecord::value)
                    .containsExactly(expectedEncoded);
        }
    }

    @Test
    void shouldForwardUnfilteredZeroAckProduceMessage(KafkaCluster cluster, Topic topic) throws Exception {

        var config = proxy(cluster);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "shouldModifyProduceMessage", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000, ACKS_CONFIG, "0"));
                var consumer = tester
                        .consumer(Serdes.String(), Serdes.String(), Map.of(GROUP_ID_CONFIG, "my-group-id", AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            producer.send(new ProducerRecord<>(topic.name(), "my-key", PLAINTEXT)).get();
            producer.flush();

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            assertThat(records).hasSize(1);
            assertThat(records.iterator())
                    .toIterable()
                    .map(ConsumerRecord::value)
                    .containsExactly(PLAINTEXT);
        }
    }

    @Test
    void shouldModifyFetchMessage(KafkaCluster cluster, Topic topic1, Topic topic2) throws Exception {

        var bytes = PLAINTEXT.getBytes(StandardCharsets.UTF_8);
        var encoded1 = encode(topic1.name(), ByteBuffer.wrap(bytes)).array();
        var encoded2 = encode(topic2.name(), ByteBuffer.wrap(bytes)).array();

        NamedFilterDefinitionBuilder filterDefinitionBuilder = new NamedFilterDefinitionBuilder("filter-1", FetchResponseTransformation.class.getName());
        var config = proxy(cluster)
                .addToFilterDefinitions(filterDefinitionBuilder
                        .withConfig("transformation", TestDecoderFactory.class.getName()).build())
                .addToDefaultFilters(filterDefinitionBuilder.name());

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Serdes.String(), Serdes.ByteArray(),
                        Map.of(CLIENT_ID_CONFIG, "shouldModifyFetchMessage", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester.consumer()) {

            producer.send(new ProducerRecord<>(topic1.name(), "my-key", encoded1)).get();
            producer.send(new ProducerRecord<>(topic2.name(), "my-key", encoded2)).get();

            consumer.subscribe(Set.of(topic1.name(), topic2.name()));
            var records = consumer.poll(Duration.ofSeconds(100));
            assertThat(records).hasSize(2);
            assertThat(records.records(topic1.name()))
                    .hasSize(1)
                    .map(ConsumerRecord::value).containsExactly(PLAINTEXT);
            assertThat(records.records(topic2.name()))
                    .hasSize(1)
                    .map(ConsumerRecord::value).containsExactly(PLAINTEXT);
        }
    }

}
