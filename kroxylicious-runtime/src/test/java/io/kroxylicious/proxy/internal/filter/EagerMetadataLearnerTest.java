/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.FilterContext;

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
                toArgs("ApiVersionsRequest", new ApiVersionsRequest(new ApiVersionsRequestData(), ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION)),
                toArgs("SaslHandshakeRequest", new SaslHandshakeRequest(new SaslHandshakeRequestData(), SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION)),
                toArgs("SaslAuthenticateRequest", new SaslAuthenticateRequest(new SaslAuthenticateRequestData(), SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("preludeRequests")
    void forwardsRequestsOfKafkaPrelude(String name, ApiKeys apiKey, RequestHeaderData header, ApiMessage request) {
        when(context.requestFilterResultBuilder()).thenReturn(new RequestFilterResultBuilderImpl());
        var stage = learner.onRequest(apiKey, header, request, context);
        assertThat(stage).isCompleted();
    }

    public static Stream<Arguments> postPreludeRequests() {
        return Stream.of(
                toArgs("ProduceRequest replaced by MetadataRequest", new ProduceRequest(new ProduceRequestData(), ProduceRequestData.HIGHEST_SUPPORTED_VERSION)),
                toArgs("MetadataRequest (highest supported)", new MetadataRequest(new MetadataRequestData(), MetadataRequestData.HIGHEST_SUPPORTED_VERSION)),
                toArgs("MetadataRequest (lowest supported)", new MetadataRequest(new MetadataRequestData(), MetadataRequestData.LOWEST_SUPPORTED_VERSION)),
                toArgs(
                        "MetadataRequest (payload fidelity)",
                        new MetadataRequest(
                                new MetadataRequestData().setTopics(List.of(new MetadataRequestTopic().setName("foo"))),
                                MetadataRequestData.LOWEST_SUPPORTED_VERSION
                        )
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("postPreludeRequests")
    void spontaneouslyEmitsMetadataRequest(String name, ApiKeys apiKey, RequestHeaderData header, ApiMessage request) throws Exception {
        var metadataResponse = new MetadataResponseData();
        metadataResponse.brokers().add(new MetadataResponseData.MetadataResponseBroker().setNodeId(1).setHost("localhost").setPort(1234));

        when(context.requestFilterResultBuilder()).thenReturn(new RequestFilterResultBuilderImpl());
        when(context.sendRequest(isA(RequestHeaderData.class), isA(MetadataRequestData.class)))
                                                                                               .thenReturn(CompletableFuture.completedStage(metadataResponse));
        var stage = learner.onRequest(apiKey, header, request, context);
        assertThat(stage).isCompleted();
        var result = stage.toCompletableFuture().get();

        if (apiKey == ApiKeys.METADATA) {
            // if caller's request is a metadata request, then the filter must forward it with fidelity
            verify(context).sendRequest(header, request);
            assertThat(result.message()).isEqualTo(metadataResponse);
        } else {
            verify(context).sendRequest(eq(new RequestHeaderData().setRequestApiVersion(MetadataRequestData.LOWEST_SUPPORTED_VERSION)), isA(MetadataRequestData.class));
        }
        assertThat(result.closeConnection()).isTrue();
    }

    private static Arguments toArgs(String name, AbstractRequest request) {
        var header = new RequestHeaderData().setRequestApiKey(request.apiKey().id).setRequestApiVersion(request.version());
        var apiKey = request.apiKey();
        var request1 = request.data();
        return Arguments.of(name, apiKey, header, request1);
    }

}
