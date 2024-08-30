/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.handler.codec.DecoderException;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.test.record.RecordTestUtils;

import static io.kroxylicious.test.assertj.ResponseAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;

class KafkaProxyExceptionHandlerTest {

    private static final SSLHandshakeException HANDSHAKE_EXCEPTION = new SSLHandshakeException("it went wrong");
    private static final short ACKS_ALL = (short) -1;
    // The special cases generally report errors on a per-entry basis rather than globally and thus need to build requests by hand
    private static final EnumSet<ApiKeys> SPECIAL_CASES = EnumSet.of(ApiKeys.LIST_OFFSETS, ApiKeys.OFFSET_FETCH, ApiKeys.METADATA, ApiKeys.UPDATE_METADATA,
            ApiKeys.JOIN_GROUP, ApiKeys.LEAVE_GROUP, ApiKeys.DESCRIBE_GROUPS, ApiKeys.CONSUMER_GROUP_DESCRIBE, ApiKeys.DELETE_GROUPS, ApiKeys.OFFSET_COMMIT,
            ApiKeys.CREATE_TOPICS, ApiKeys.DELETE_TOPICS, ApiKeys.DELETE_RECORDS, ApiKeys.INIT_PRODUCER_ID, ApiKeys.CREATE_ACLS, ApiKeys.DESCRIBE_ACLS,
            ApiKeys.DELETE_ACLS, ApiKeys.OFFSET_FOR_LEADER_EPOCH, ApiKeys.ELECT_LEADERS, ApiKeys.ADD_PARTITIONS_TO_TXN, ApiKeys.WRITE_TXN_MARKERS,
            ApiKeys.TXN_OFFSET_COMMIT, ApiKeys.DESCRIBE_CONFIGS, ApiKeys.ALTER_CONFIGS, ApiKeys.INCREMENTAL_ALTER_CONFIGS, ApiKeys.ALTER_REPLICA_LOG_DIRS,
            ApiKeys.CREATE_PARTITIONS, ApiKeys.ALTER_CLIENT_QUOTAS, ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS, ApiKeys.ALTER_USER_SCRAM_CREDENTIALS,
            ApiKeys.DESCRIBE_PRODUCERS, ApiKeys.DESCRIBE_TRANSACTIONS, ApiKeys.DESCRIBE_TOPIC_PARTITIONS);

    private static final Map<ApiKeys, Consumer<ApiMessage>> messagePopulators = Map.of(
            ApiKeys.PRODUCE, (KafkaProxyExceptionHandlerTest::populateProduceRequest));

    private KafkaProxyExceptionHandler kafkaProxyExceptionHandler;

    @BeforeEach
    void setUp() {
        kafkaProxyExceptionHandler = new KafkaProxyExceptionHandler();
    }

    @Test
    void shouldCloseWithForRegisteredException() {
        // Given
        kafkaProxyExceptionHandler.registerExceptionResponse(SSLHandshakeException.class, throwable -> Optional.of(new UnknownServerException(throwable)));

        // When
        final Optional<?> result = kafkaProxyExceptionHandler.handleException(HANDSHAKE_EXCEPTION);

        // Then
        assertThat(result).isNotEmpty().get().isInstanceOf(UnknownServerException.class);
    }

    @Test
    void shouldUnwrapCauseToFindForRegisteredException() {
        // Given
        kafkaProxyExceptionHandler.registerExceptionResponse(SSLHandshakeException.class, throwable -> Optional.of(new UnknownServerException(throwable)));

        // When
        final Optional<?> result = kafkaProxyExceptionHandler.handleException(new DecoderException(HANDSHAKE_EXCEPTION));

        // Then
        assertThat(result).isNotEmpty().get().isInstanceOf(UnknownServerException.class);
    }

    @ParameterizedTest
    @MethodSource({ "decodedFrameSourceLatestVersion", "decodedFrameSourceOldestVersion" })
    void shouldGenerateErrorResponseApiKey(DecodedRequestFrame<?> request) {
        // Given
        // When
        final AbstractResponse response = kafkaProxyExceptionHandler.errorResponse(request, new BrokerNotAvailableException("handshake failure", HANDSHAKE_EXCEPTION));

        // Then
        assertThat(response)
                .hasApiKey(request.apiKey())
                .hasErrorCount(Errors.BROKER_NOT_AVAILABLE, 1);
    }

    static Stream<Arguments> decodedFrameSourceLatestVersion() {
        return decodedFrameSource(ApiKeys::latestVersion);
    }

    static Stream<Arguments> decodedFrameSourceOldestVersion() {
        return decodedFrameSource(ApiKeys::oldestVersion);
    }

    static Stream<Arguments> decodedFrameSource(Function<ApiKeys, Short> versionFunction) {
        return Stream.of(EnumSet.complementOf(SPECIAL_CASES))
                .flatMap(Collection::stream)
                .map(apiKey -> {
                    final RequestHeaderData requestHeaderData = new RequestHeaderData();
                    requestHeaderData.setCorrelationId(124);
                    final ApiMessage apiMessage = apiKey.messageType.newRequest();
                    messagePopulators.getOrDefault(apiKey, message -> {
                    }).accept(apiMessage);

                    final Short apiVersion = versionFunction.apply(apiKey);
                    return named(apiKey + "-v" + apiVersion, new DecodedRequestFrame<>(apiVersion, 1, false, requestHeaderData, apiMessage));
                })
                .map(Arguments::of);
    }

    private static void populateProduceRequest(ApiMessage apiMessage) {
        final ProduceRequestData produceRequestData = (ProduceRequestData) apiMessage;
        produceRequestData.setAcks(ACKS_ALL);
        final ProduceRequestData.TopicProduceDataCollection v = new ProduceRequestData.TopicProduceDataCollection(1);
        final ProduceRequestData.TopicProduceData topicProduceData = new ProduceRequestData.TopicProduceData();
        final ProduceRequestData.PartitionProduceData produceData = new ProduceRequestData.PartitionProduceData();
        produceData.setRecords(RecordTestUtils.memoryRecords(List.of(RecordTestUtils.record("wibble"))));
        topicProduceData.setPartitionData(List.of(produceData));
        v.add(topicProduceData);
        produceRequestData.setTopicData(v);
    }

}