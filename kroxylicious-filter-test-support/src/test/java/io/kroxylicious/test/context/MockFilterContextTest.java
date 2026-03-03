/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.context;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;
import io.kroxylicious.proxy.tls.ClientTlsContext;
import io.kroxylicious.test.assertj.MockFilterContextAssert;
import io.kroxylicious.test.context.MockFilterContext.ClientSaslGestureInvocation.AuthenticationFailure;
import io.kroxylicious.test.context.MockFilterContext.ClientSaslGestureInvocation.AuthenticationSuccess;
import io.kroxylicious.test.context.MockFilterContext.ClientSaslGestureInvocation.DeprecatedAuthenticationSuccess;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MockFilterContextTest {

    private static final RequestHeaderData HEADER = new RequestHeaderData();
    private static final ApiMessage MESSAGE = new RequestHeaderData();

    @Test
    void shouldBuildSuccessfully() {
        // given
        MockFilterContext.MockFilterContextBuilder builder = MockFilterContext.builder(HEADER, MESSAGE);

        // when
        MockFilterContext context = builder.build();

        // then
        assertThat(context).isNotNull();
    }

    @Test
    void builderShouldRejectNullConstructorArgs() {
        // given / when / then
        assertThatThrownBy(() -> MockFilterContext.builder(null, MESSAGE))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("header must not be null");

        assertThatThrownBy(() -> MockFilterContext.builder(HEADER, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("message not be null");
    }

    @Test
    void builderShouldAddTopicNames() {
        // given
        Uuid topicId = Uuid.randomUuid();
        String topicName = "my-topic";

        // when
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE)
                .withTopicName(topicId, topicName)
                .withTopicNames(Map.of(Uuid.randomUuid(), "another-topic"))
                .build();

        // then
        assertThat(context.topicNames(List.of(topicId)).toCompletableFuture().join().topicNames())
                .containsEntry(topicId, topicName);
    }

    @Test
    void builderShouldRejectZeroUuidForTopicId() {
        // given
        MockFilterContext.MockFilterContextBuilder builder = MockFilterContext.builder(HEADER, MESSAGE);

        // when / then
        assertThatThrownBy(() -> builder.withTopicName(Uuid.ZERO_UUID, "invalid-topic"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("avoid using ZERO_UUID");
    }

    @Test
    void builderShouldRejectDuplicateTopicIds() {
        // given
        Uuid topicId = Uuid.randomUuid();
        MockFilterContext.MockFilterContextBuilder builder = MockFilterContext.builder(HEADER, MESSAGE)
                .withTopicName(topicId, "first-name");

        // when / then
        assertThatThrownBy(() -> builder.withTopicName(topicId, "second-name"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already has name");
    }

    @Test
    void shouldReturnDefaultContextProperties() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when / then
        assertThat(context.channelDescriptor()).isEqualTo("channelDescriptor");
        assertThat(context.sessionId()).isEqualTo("sessionId");
        assertThat(context.sniHostname()).isEqualTo("sniHostname");
        assertThat(context.getVirtualClusterName()).isEqualTo("virtualCluster");
        assertThat(context.clientSaslContext()).isEmpty();
        assertThat(context.clientTlsContext()).isEmpty();
        assertThat(context.authenticatedSubject()).isEqualTo(Subject.anonymous());
    }

    @Test
    void shouldReturnOverriddenChannelDescriptor() {
        // given
        String newChannelDescriptor = "other";
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withChannelDescriptor(newChannelDescriptor).build();

        // when / then
        assertThat(context.channelDescriptor()).isEqualTo(newChannelDescriptor);
    }

    @Test
    void shouldReturnOverriddenSessionId() {
        // given
        String newSessionId = "session";
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withSessionId(newSessionId).build();

        // when / then
        assertThat(context.sessionId()).isEqualTo(newSessionId);
    }

    @Test
    void shouldReturnOverriddenSniHostname() {
        // given
        String newSniHostname = "new.sni";
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withSniHostname(newSniHostname).build();

        // when / then
        assertThat(context.sniHostname()).isEqualTo(newSniHostname);
    }

    @Test
    void shouldReturnOverriddenVirtualClusterName() {
        // given
        String newVirtualClusterName = "my-cluster";
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withVirtualClusterName(newVirtualClusterName).build();

        // when / then
        assertThat(context.getVirtualClusterName()).isEqualTo(newVirtualClusterName);
    }

    @Test
    void shouldReturnOverriddenClientSaslContextDetails() {
        // given
        String mechanism = "mechanism";
        String authorizationId = "alice";
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withClientSaslContext(mechanism, authorizationId).build();

        // when / then
        assertThat(context.clientSaslContext()).isNotEmpty().hasValueSatisfying(clientSaslContext -> {
            assertThat(clientSaslContext.mechanismName()).isEqualTo(mechanism);
            assertThat(clientSaslContext.authorizationId()).isEqualTo(authorizationId);
        });
    }

    @Test
    void shouldReturnOverriddenClientTlContextDetails() {
        // given
        X509Certificate serverCert = Mockito.mock(X509Certificate.class);
        X509Certificate clientCert = Mockito.mock(X509Certificate.class);
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withClientTlsContext(serverCert, clientCert).build();

        // when / then
        assertThat(context.clientTlsContext()).isNotEmpty().hasValueSatisfying(clientTlsContext -> {
            assertThat(clientTlsContext.proxyServerCertificate()).isSameAs(serverCert);
            assertThat(clientTlsContext.clientCertificate()).isNotEmpty().hasValueSatisfying(x509Certificate -> {
                assertThat(x509Certificate).isSameAs(clientCert);
            });
        });
    }

    @Test
    void shouldReturnOverriddenClientTlContext() {
        // given
        ClientTlsContext tlsContext = Mockito.mock(ClientTlsContext.class);
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withClientTlsContext(tlsContext).build();

        // when / then
        assertThat(context.clientTlsContext()).isNotEmpty().hasValueSatisfying(clientTlsContext -> {
            assertThat(clientTlsContext).isSameAs(tlsContext);
        });
    }

    @Test
    void shouldReturnOverriddenAuthenticatedSubject() {
        // given
        Subject subject = new Subject(new User("happy"));
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withAuthenticatedSubject(subject).build();

        // when / then
        assertThat(context.authenticatedSubject()).isEqualTo(subject);
    }

    @Test
    void shouldReturnOverriddenClientSaslContext() {
        // given
        ClientSaslContext overrideContext = new ClientSaslContext() {
            @NonNull
            @Override
            public String mechanismName() {
                return "a";
            }

            @NonNull
            @Override
            public String authorizationId() {
                return "b";
            }
        };
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withClientSaslContext(overrideContext).build();

        // when / then
        assertThat(context.clientSaslContext()).isNotEmpty().hasValueSatisfying(clientSaslContext -> assertThat(clientSaslContext).isSameAs(overrideContext));
    }

    @Test
    void shouldCreateByteBufferOutputStream() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ByteBufferOutputStream stream = context.createByteBufferOutputStream(128);

        // then
        assertThat(stream).isNotNull();
        assertThat(stream.limit()).isEqualTo(128);
    }

    @Test
    void shouldResolveTopicNames() {
        // given
        Uuid knownId = Uuid.randomUuid();
        Uuid unknownId = Uuid.randomUuid();
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE)
                .withTopicName(knownId, "known-topic")
                .build();

        // when
        TopicNameMapping mapping = context.topicNames(List.of(knownId, unknownId)).toCompletableFuture().join();

        // then
        assertThat(mapping.anyFailures()).isTrue();
        assertThat(mapping.topicNames())
                .hasSize(1)
                .containsEntry(knownId, "known-topic");
        assertThat(mapping.failures())
                .hasSize(1)
                .containsKey(unknownId)
                .hasEntrySatisfying(unknownId, e -> assertThat(e).hasMessageContaining("no mapping for topicId configured in MockFilterContext"));
    }

    @Test
    void shouldResolveTopicNamesErrors() {
        // given
        Uuid knownId = Uuid.randomUuid();
        Uuid errorId = Uuid.randomUuid();
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE)
                .withTopicName(knownId, "known-topic")
                .withTopicNameError(errorId, new TopicNameMappingException(Errors.UNKNOWN_TOPIC_ID, "unknown topic id"))
                .build();

        // when
        TopicNameMapping mapping = context.topicNames(List.of(knownId, errorId)).toCompletableFuture().join();

        // then
        assertThat(mapping.anyFailures()).isTrue();
        assertThat(mapping.topicNames())
                .hasSize(1)
                .containsEntry(knownId, "known-topic");
        assertThat(mapping.failures())
                .hasSize(1)
                .containsKey(errorId)
                .hasEntrySatisfying(errorId, e -> {
                    assertThat(e).hasMessageContaining("unknown topic id");
                    assertThat(e.getError()).isEqualTo(Errors.UNKNOWN_TOPIC_ID);
                });
    }

    @Test
    void shouldReturnNoFailuresWhenAllTopicsFound() {
        // given
        Uuid knownId = Uuid.randomUuid();
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE)
                .withTopicName(knownId, "known-topic")
                .build();

        // when
        TopicNameMapping mapping = context.topicNames(List.of(knownId)).toCompletableFuture().join();

        // then
        assertThat(mapping.anyFailures()).isFalse();
        assertThat(mapping.topicNames()).containsEntry(knownId, "known-topic");
        assertThat(mapping.failures()).isEmpty();
    }

    @Test
    void shouldBuildForwardRequestFilterResult() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ApiVersionsRequestData newData = new ApiVersionsRequestData();
        RequestHeaderData header = new RequestHeaderData();
        RequestFilterResult result = context.requestFilterResultBuilder()
                .forward(header, newData)
                .build();

        // then
        MockFilterContextAssert.assertThat(result)
                .isForwardRequest()
                .isNotShortCircuitResponse()
                .isNotErrorResponse()
                .isNotCloseConnection()
                .isNotDropRequest()
                .hasHeaderEqualTo(header)
                .hasMessageEqualTo(newData);
    }

    @Test
    void shouldBuildForwardRequestFilterResultCompleted() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ApiVersionsRequestData newData = new ApiVersionsRequestData();
        RequestHeaderData header = new RequestHeaderData();
        CompletionStage<RequestFilterResult> result = context.requestFilterResultBuilder()
                .forward(header, newData)
                .completed();

        // then
        assertThat(result).succeedsWithin(Duration.ZERO).satisfies(requestFilterResult -> MockFilterContextAssert.assertThat(requestFilterResult)
                .isNotShortCircuitResponse()
                .isNotErrorResponse()
                .isNotCloseConnection()
                .isNotDropRequest()
                .hasHeaderEqualTo(header)
                .hasMessageEqualTo(newData));
    }

    @Test
    void shouldBuildShortCircuitRequestFilterResult() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ApiMessage newData = new ApiVersionsRequestData();
        ResponseHeaderData newRequestHeaderData = new ResponseHeaderData();
        RequestFilterResult result = context.requestFilterResultBuilder()
                .shortCircuitResponse(newRequestHeaderData, newData)
                .build();

        // then
        MockFilterContextAssert.assertThat(result).isShortCircuitResponse()
                .isNotForwardRequest()
                .hasHeaderEqualTo(newRequestHeaderData)
                .hasMessageEqualTo(newData)
                .isNotCloseConnection();
    }

    @Test
    void shouldBuildShortCircuitWithCloseConnectionRequestFilterResult() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ApiMessage newData = new ApiVersionsRequestData();
        ResponseHeaderData newRequestHeaderData = new ResponseHeaderData();
        RequestFilterResult result = context.requestFilterResultBuilder()
                .shortCircuitResponse(newRequestHeaderData, newData)
                .withCloseConnection()
                .build();

        // then
        MockFilterContextAssert.assertThat(result).isShortCircuitResponse()
                .hasHeaderEqualTo(newRequestHeaderData)
                .hasMessageEqualTo(newData)
                .isCloseConnection();
    }

    @Test
    void shouldBuildCloseConnectionRequestFilterResult() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        RequestFilterResult result = context.requestFilterResultBuilder()
                .withCloseConnection()
                .build();

        // then
        MockFilterContextAssert.assertThat(result).isCloseConnection();
    }

    @Test
    void shouldBuildShortCircuitRequestFilterResultWithOnlyMessage() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ApiMessage newData = new ApiVersionsRequestData();
        RequestFilterResult result = context.requestFilterResultBuilder()
                .shortCircuitResponse(newData)
                .build();

        // then
        MockFilterContextAssert.assertThat(result)
                .isShortCircuitResponse()
                .hasHeaderEqualTo(HEADER)
                .hasMessageEqualTo(newData);
    }

    @Test
    void shouldBuildErrorRequestFilterResult() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();
        ApiException exception = new UnknownServerException("Test Error");

        // when
        RequestFilterResult result = context.requestFilterResultBuilder()
                .errorResponse(HEADER, MESSAGE, exception)
                .build();

        // then
        MockFilterContextAssert.assertThat(result)
                .isNotCloseConnection()
                .isShortCircuitResponse()
                .isErrorResponse()
                .isNotDropRequest()
                .errorResponse().isEqualTo(exception);
    }

    @Test
    void shouldBuildErrorRequestFilterResultCompleted() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();
        ApiException exception = new UnknownServerException("Test Error");

        // when
        CompletionStage<RequestFilterResult> result = context.requestFilterResultBuilder()
                .errorResponse(HEADER, MESSAGE, exception)
                .completed();

        // then
        assertThat(result).succeedsWithin(Duration.ZERO).satisfies(requestFilterResult -> MockFilterContextAssert.assertThat(requestFilterResult)
                .isNotCloseConnection()
                .isShortCircuitResponse()
                .isErrorResponse()
                .isNotDropRequest()
                .errorResponse().isEqualTo(exception));
    }

    @Test
    void shouldBuildErrorWithCloseConnectionRequestFilterResult() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();
        ApiException exception = new UnknownServerException("Test Error");

        // when
        RequestFilterResult result = context.requestFilterResultBuilder()
                .errorResponse(HEADER, MESSAGE, exception)
                .withCloseConnection()
                .build();

        // then
        MockFilterContextAssert.assertThat(result)
                .isCloseConnection()
                .isShortCircuitResponse()
                .isErrorResponse()
                .isNotDropRequest()
                .errorResponse().isEqualTo(exception);
    }

    @Test
    void shouldBuildErrorWithCloseConnectionRequestFilterResultCompleted() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();
        ApiException exception = new UnknownServerException("Test Error");

        // when
        CompletionStage<RequestFilterResult> result = context.requestFilterResultBuilder()
                .errorResponse(HEADER, MESSAGE, exception)
                .withCloseConnection()
                .completed();

        // then

        assertThat(result).succeedsWithin(Duration.ZERO).satisfies(requestFilterResult -> MockFilterContextAssert.assertThat(requestFilterResult)
                .isCloseConnection()
                .isShortCircuitResponse()
                .isErrorResponse()
                .isNotDropRequest()
                .errorResponse().isEqualTo(exception));
    }

    @Test
    void shouldBuildDropRequestFilterResult() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        RequestFilterResult result = context.requestFilterResultBuilder()
                .drop()
                .build();

        // then
        MockFilterContextAssert.assertThat(result).isDropRequest().isNotForwardRequest();
    }

    @Test
    void shouldBuildDropRequestFilterResultCompleted() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        CompletionStage<RequestFilterResult> result = context.requestFilterResultBuilder()
                .drop()
                .completed();

        // then

        assertThat(result).succeedsWithin(Duration.ZERO)
                .satisfies(requestFilterResult -> MockFilterContextAssert.assertThat(requestFilterResult).isDropRequest());
    }

    @Test
    void shouldBuildForwardResponseFilterResult() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ResponseHeaderData newResponseHeaderData = new ResponseHeaderData();
        ApiVersionsResponseData newData = new ApiVersionsResponseData();
        ResponseFilterResult result = context.responseFilterResultBuilder()
                .forward(newResponseHeaderData, newData)
                .build();

        // then
        MockFilterContextAssert.assertThat(result)
                .isNotCloseConnection()
                .isNotDropResponse()
                .hasHeaderEqualTo(newResponseHeaderData)
                .hasHeaderInstanceOfSatisfying(ResponseHeaderData.class, responseHeaderData -> assertThat(responseHeaderData).isEqualTo(newResponseHeaderData))
                .hasMessageEqualTo(newData)
                .hasMessageInstanceOfSatisfying(ApiVersionsResponseData.class, apiVersionsResponseData -> assertThat(apiVersionsResponseData).isEqualTo(newData));
    }

    @Test
    void shouldBuildForwardResponseFilterResultCompleted() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ResponseHeaderData newResponseHeaderData = new ResponseHeaderData();
        ApiVersionsResponseData newData = new ApiVersionsResponseData();
        CompletionStage<ResponseFilterResult> result = context.responseFilterResultBuilder()
                .forward(newResponseHeaderData, newData)
                .completed();

        // then
        assertThat(result).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> MockFilterContextAssert.assertThat(responseFilterResult)
                .isNotCloseConnection()
                .isNotDropResponse()
                .hasHeaderEqualTo(newResponseHeaderData)
                .hasMessageEqualTo(newData));
    }

    @Test
    void shouldBuildForwardWithCloseConnectionResponseFilterResult() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ResponseHeaderData newResponseHeaderData = new ResponseHeaderData();
        ApiVersionsResponseData newData = new ApiVersionsResponseData();
        ResponseFilterResult result = context.responseFilterResultBuilder()
                .forward(newResponseHeaderData, newData)
                .withCloseConnection()
                .build();

        // then
        MockFilterContextAssert.assertThat(result)
                .isCloseConnection()
                .isNotDropResponse()
                .hasHeaderEqualTo(newResponseHeaderData)
                .hasMessageEqualTo(newData);
    }

    @Test
    void shouldBuildForwardWithCloseConnectionResponseFilterResultCompleted() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ResponseHeaderData newResponseHeaderData = new ResponseHeaderData();
        ApiVersionsResponseData newData = new ApiVersionsResponseData();
        CompletionStage<ResponseFilterResult> result = context.responseFilterResultBuilder()
                .forward(newResponseHeaderData, newData)
                .withCloseConnection()
                .completed();

        // then
        assertThat(result).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> MockFilterContextAssert.assertThat(responseFilterResult)
                .isCloseConnection()
                .isNotDropResponse()
                .hasHeaderEqualTo(newResponseHeaderData)
                .hasMessageEqualTo(newData));
    }

    @Test
    void shouldBuildDropResponseFilterResult() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ResponseFilterResult result = context.responseFilterResultBuilder()
                .drop()
                .build();

        // then
        MockFilterContextAssert.assertThat(result).isDropResponse().isNotForwardResponse();
    }

    @Test
    void shouldBuildCloseConnectionResponseFilterResult() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ResponseFilterResult result = context.responseFilterResultBuilder()
                .withCloseConnection()
                .build();

        // then
        MockFilterContextAssert.assertThat(result).isCloseConnection();
    }

    @Test
    void shouldCompleteForwardRequestDirectly() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        RequestHeaderData newHeader = new RequestHeaderData();
        ApiVersionsRequestData newData = new ApiVersionsRequestData();
        CompletionStage<RequestFilterResult> stage = context.forwardRequest(newHeader, newData);

        // then
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(requestFilterResult -> MockFilterContextAssert.assertThat(requestFilterResult)
                .hasMessageEqualTo(newData)
                .hasMessageInstanceOfSatisfying(ApiVersionsRequestData.class, data -> {
                    assertThat(data).isSameAs(newData);
                })
                .isForwardRequest()
                .hasHeaderInstanceOfSatisfying(RequestHeaderData.class, requestHeaderData -> {
                    assertThat(requestHeaderData).isSameAs(newHeader);
                })
                .hasHeaderEqualTo(newHeader)
                .isNotCloseConnection()
                .isNotShortCircuitResponse()
                .isNotDropRequest()
                .isNotErrorResponse());
    }

    @Test
    void shouldCompleteForwardResponseDirectly() {
        // given
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();

        // when
        ResponseHeaderData newHeader = new ResponseHeaderData();
        ApiVersionsRequestData newData = new ApiVersionsRequestData();
        CompletionStage<ResponseFilterResult> stage = context.forwardResponse(newHeader, newData);

        // then
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> MockFilterContextAssert.assertThat(responseFilterResult)
                .hasMessageEqualTo(newData)
                .hasHeaderEqualTo(newHeader)
                .isNotCloseConnection()
                .isForwardResponse()
                .isNotDropResponse());
    }

    @Test
    void sendRequest() {
        // given
        ApiVersionsResponseData response = new ApiVersionsResponseData();
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withSendRequestResponseEnqueued(response).build();

        // when
        CompletionStage<ApiMessage> stage = context.sendRequest(HEADER, MESSAGE);

        // then
        assertThat(stage).succeedsWithin(Duration.ZERO).isEqualTo(response);
        assertThat(context.sendRequestInvocations()).hasSize(1).containsExactly(new MockFilterContext.SendRequestInvocation(HEADER, MESSAGE));
    }

    @Test
    void sendRequestEquals() {
        // given
        ApiVersionsResponseData response = new ApiVersionsResponseData();
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withSendRequestResponseEnqueued(HEADER, MESSAGE, response).build();

        // when
        CompletionStage<ApiMessage> stage = context.sendRequest(HEADER, MESSAGE);

        // then
        assertThat(stage).succeedsWithin(Duration.ZERO).isEqualTo(response);
        assertThat(context.sendRequestInvocations()).hasSize(1).containsExactly(new MockFilterContext.SendRequestInvocation(HEADER, MESSAGE));
    }

    @Test
    void sendRequestRequestNotEquals() {
        // given
        ApiVersionsResponseData response = new ApiVersionsResponseData();
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withSendRequestResponseEnqueued(HEADER, MESSAGE, response).build();

        // when / then
        ApiVersionsRequestData otherData = new ApiVersionsRequestData();
        assertThatThrownBy(() -> context.sendRequest(HEADER, otherData)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("request being passed to sendRequest did not equal expected request");
    }

    @Test
    void sendRequestHeaderNotEquals() {
        // given
        ApiVersionsResponseData response = new ApiVersionsResponseData();
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).withSendRequestResponseEnqueued(HEADER, MESSAGE, response).build();

        // when / then
        RequestHeaderData other = new RequestHeaderData().setClientId("other");
        assertThatThrownBy(() -> context.sendRequest(other, MESSAGE)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("header being passed to sendRequest did not equal expected header");
    }

    @Test
    void recordsDeprecatedSaslSuccess() {
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();
        context.clientSaslAuthenticationSuccess("mechanism", "authorizedId");

        assertThat(context.clientSaslGestureInvocations()).hasSize(1)
                .containsExactly(new DeprecatedAuthenticationSuccess("mechanism", "authorizedId"));
    }

    @Test
    void recordsSaslSuccess() {
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();
        context.clientSaslAuthenticationSuccess("mechanism", Subject.anonymous());

        assertThat(context.clientSaslGestureInvocations()).hasSize(1)
                .containsExactly(new AuthenticationSuccess("mechanism", Subject.anonymous()));
    }

    @Test
    void recordsSaslFailed() {
        MockFilterContext context = MockFilterContext.builder(HEADER, MESSAGE).build();
        RuntimeException exception = new RuntimeException();
        context.clientSaslAuthenticationFailure("mechanism", "authorizedId", exception);

        assertThat(context.clientSaslGestureInvocations()).hasSize(1)
                .containsExactly(new AuthenticationFailure("mechanism", "authorizedId", exception));
    }
}