/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslHandshakeRequestData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.internal.filter.impl.EagerMetadataLearner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EagerMetadataLearnerTest {

    @Mock
    FilterContext context;
    private EagerMetadataLearner learner;

    @BeforeEach
    void setUp() {
        learner = new EagerMetadataLearner();
    }

    public static Stream<Arguments> preludeRequests() {
        return Stream.of(
                toArgs("ApiVersionsRequest", ApiKeys.API_VERSIONS, ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION, new ApiVersionsRequestData()),
                toArgs("SaslHandshakeRequest", ApiKeys.SASL_HANDSHAKE, SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new SaslHandshakeRequestData()),
                toArgs("SaslAuthenticateRequest", ApiKeys.SASL_AUTHENTICATE, SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION, new SaslAuthenticateRequestData()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("preludeRequests")
    void forwardsRequestsOfKafkaPrelude(String name, ApiKeys apiKey, RequestHeaderData header, ApiMessage request) {
        when(context.requestFilterResultBuilder()).thenReturn(new RequestFilterResultBuilderImpl());
        var stage = learner.onRequest(apiKey, header.requestApiVersion(), header, request, context);
        assertThat(stage).isCompleted();
    }

    public static Stream<Arguments> postPreludeRequests() {
        return Stream.of(
                toArgs("ProduceRequest replaced by MetadataRequest", ApiKeys.PRODUCE, ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new ProduceRequestData()),
                toArgs("MetadataRequest (highest supported)", ApiKeys.METADATA, MetadataRequestData.HIGHEST_SUPPORTED_VERSION, new MetadataRequestData()),
                toArgs("MetadataRequest (lowest supported)", ApiKeys.METADATA, MetadataRequestData.LOWEST_SUPPORTED_VERSION, new MetadataRequestData()),
                toArgs("MetadataRequest (payload fidelity)", ApiKeys.METADATA, MetadataRequestData.LOWEST_SUPPORTED_VERSION,
                        new MetadataRequestData().setTopics(List.of(new MetadataRequestTopic().setName("foo")))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("postPreludeRequests")
    void spontaneouslyEmitsMetadataRequest(String name, ApiKeys apiKey, RequestHeaderData header, ApiMessage request) throws Exception {
        var metadataResponse = new MetadataResponseData();
        metadataResponse.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(1).setHost("localhost").setPort(1234));

        when(context.requestFilterResultBuilder()).thenReturn(new RequestFilterResultBuilderImpl());
        when(context.sendRequest(isA(RequestHeaderData.class), isA(MetadataRequestData.class)))
                .thenReturn(CompletableFuture.completedStage(metadataResponse));
        var stage = learner.onRequest(apiKey, header.requestApiVersion(), header, request, context);
        assertThat(stage).isCompleted();
        var result = stage.toCompletableFuture().get();

        if (apiKey == ApiKeys.METADATA) {
            // if caller's request is a metadata request, then the filter must forward it with fidelity
            verify(context).sendRequest(header, request);
            assertThat(result.message()).isEqualTo(metadataResponse);
        }
        else {
            verify(context).sendRequest(eq(new RequestHeaderData().setRequestApiVersion(MetadataRequestData.LOWEST_SUPPORTED_VERSION)), isA(MetadataRequestData.class));
        }
        assertThat(result.closeConnection()).isTrue();
    }

    private static Arguments toArgs(String name, ApiKeys apiKey, short version, ApiMessage requestData) {
        var header = new RequestHeaderData().setRequestApiKey(apiKey.id).setRequestApiVersion(version);
        return Arguments.of(name, apiKey, header, requestData);
    }

}
