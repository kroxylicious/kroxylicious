/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.filter.context;

import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.assertj.core.util.Lists;

import com.google.common.collect.Maps;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.kafka.common.utils.ByteBufferOutputStream;
import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResultBuilder;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Mock implementation of {@link FilterContext}. The intent is to mock the behaviour of the real
 * implementation.
 * <p>
 * The only behaviour that is not easy to replicate is the errorResponse on RequestFilterResultBuilder. This instructs
 * the framework to build an error response. In this case the MockFilterContext will record what was used.
 * <p>
 * <strong>Why?</strong>
 * Re-implementing the FilterContext using Mockito for every filter test increased the likelihood
 * of mistakes being introduced. It was also very verbose since we use numerous classes to provide a typesafe API.
 */
public class MockFilterContext implements FilterContext {

    /** Channel descriptor used unless overridden on the builder. */
    public static final String DEFAULT_CHANNEL_DESCRIPTOR = "channelDescriptor";
    /** Session id used unless overridden on the builder. */
    public static final String DEFAULT_SESSION_ID = "sessionId";
    /** SNI hostname used unless overridden on the builder. */
    public static final String DEFAULT_SNI_HOSTNAME = "sniHostname";
    /** Virtual cluster name used unless overridden on the builder. */
    public static final String DEFAULT_VIRTUAL_CLUSTER_NAME = "virtualCluster";
    /** Authenticated subject used unless overridden on the builder. */
    public static final Subject DEFAULT_AUTHENTICATED_SUBJECT = Subject.anonymous();

    private static TopicNameMappingException notConfiguredException() {
        return new TopicNameMappingException(Errors.UNKNOWN_SERVER_ERROR, "no mapping for topicId configured in MockFilterContext");
    }

    private final ApiMessage header;
    private final ApiMessage message;
    private final Map<Uuid, String> topicNames;
    private final String channelDescriptor;
    private final String sessionId;
    @Nullable
    private final String sniHostname;
    private final String virtualClusterName;
    @Nullable
    private final ClientSaslContext clientSaslContext;
    @Nullable
    private final ClientTlsContext clientTlsContext;
    private final Subject authenticatedSubject;
    private final List<MockSendRequestResponse> sendRequestResponses;
    private final List<SendRequestInvocation> sendRequestInvocations = new CopyOnWriteArrayList<>();
    private final List<ClientSaslGestureInvocation> clientSaslGestureInvocations = new CopyOnWriteArrayList<>();
    private final Map<Uuid, TopicNameMappingException> topicNameFailures;

    @SuppressWarnings("java:S107") // large constructor justified
    private MockFilterContext(ApiMessage header,
                              ApiMessage message,
                              Map<Uuid, String> topicNames,
                              Map<Uuid, TopicNameMappingException> topicNameFailures,
                              String channelDescriptor,
                              String sessionId,
                              @Nullable String sniHostname,
                              String virtualClusterName,
                              @Nullable ClientSaslContext clientSaslContext,
                              @Nullable ClientTlsContext clientTlsContext,
                              Subject authenticatedSubject,
                              List<MockSendRequestResponse> sendRequestResponses) {
        this.sendRequestResponses = sendRequestResponses;
        Objects.requireNonNull(header, "header must not be null");
        Objects.requireNonNull(message, "message must not be null");
        Objects.requireNonNull(topicNames, "topicNames must not be null");
        Objects.requireNonNull(topicNameFailures, "topicNameFailures must not be null");
        Objects.requireNonNull(channelDescriptor, "channelDescriptor must not be null");
        Objects.requireNonNull(sessionId, "sessionId must not be null");
        Objects.requireNonNull(virtualClusterName, "virtualClusterName must not be null");
        Objects.requireNonNull(authenticatedSubject, "authenticatedSubject must not be null");
        this.header = header;
        this.message = message;
        this.topicNames = topicNames;
        this.topicNameFailures = topicNameFailures;
        this.channelDescriptor = channelDescriptor;
        this.sessionId = sessionId;
        this.sniHostname = sniHostname;
        this.virtualClusterName = virtualClusterName;
        this.clientSaslContext = clientSaslContext;
        this.clientTlsContext = clientTlsContext;
        this.authenticatedSubject = authenticatedSubject;
    }

    /**
     * Creates a builder for a MockFilterContext handling the given request or response.
     *
     * @param header the header of the message being filtered
     * @param message the body of the message being filtered
     * @return the builder
     */
    public static MockFilterContextBuilder builder(ApiMessage header, ApiMessage message) {
        return new MockFilterContextBuilder(header, message);
    }

    /**
     * Builder for {@link MockFilterContext} instances.
     */
    public static class MockFilterContextBuilder {
        private final ApiMessage header;
        private final ApiMessage message;
        private final Map<Uuid, String> topicNames = Maps.newHashMap();
        private final Map<Uuid, TopicNameMappingException> topicNameFailures = Maps.newHashMap();
        private String channelDescriptor = DEFAULT_CHANNEL_DESCRIPTOR;
        private String sessionId = DEFAULT_SESSION_ID;
        private @Nullable String sniHostname = DEFAULT_SNI_HOSTNAME;
        private String virtualClusterName = DEFAULT_VIRTUAL_CLUSTER_NAME;
        @Nullable
        private ClientSaslContext clientSaslContext = null;
        @Nullable
        private ClientTlsContext clientTlsContext = null;
        private Subject authenticatedSubject = DEFAULT_AUTHENTICATED_SUBJECT;
        private final List<MockSendRequestResponse> sendRequestResponses = Lists.newArrayList();

        /**
         * Constructs a builder for a MockFilterContext handling the given request or response.
         *
         * @param header the header of the message being filtered
         * @param message the body of the message being filtered
         */
        public MockFilterContextBuilder(ApiMessage header, ApiMessage message) {
            Objects.requireNonNull(header, "header must not be null");
            Objects.requireNonNull(message, "message not be null");
            this.header = header;
            this.message = message;
        }

        /**
         * Builds the MockFilterContext.
         *
         * @return the MockFilterContext
         */
        public MockFilterContext build() {
            return new MockFilterContext(header,
                    message,
                    topicNames,
                    topicNameFailures,
                    channelDescriptor,
                    sessionId,
                    sniHostname,
                    virtualClusterName,
                    clientSaslContext,
                    clientTlsContext,
                    authenticatedSubject,
                    sendRequestResponses);
        }

        /**
         * Configures the topic id to name mappings returned by {@link FilterContext#topicNames(Collection)}.
         *
         * @param topicNames the topic id to name mappings
         * @return this builder
         */
        public MockFilterContextBuilder withTopicNames(Map<Uuid, String> topicNames) {
            Objects.requireNonNull(topicNames);
            for (Map.Entry<Uuid, String> entry : topicNames.entrySet()) {
                withTopicName(entry.getKey(), entry.getValue());
            }
            return this;
        }

        /**
         * Configures a topic id to name mapping returned by {@link FilterContext#topicNames(Collection)}.
         *
         * @param topicId the topic id
         * @param topicName the topic name
         * @return this builder
         */
        public MockFilterContextBuilder withTopicName(Uuid topicId, String topicName) {
            Objects.requireNonNull(topicId);
            Objects.requireNonNull(topicName);
            if (Uuid.ZERO_UUID.equals(topicId)) {
                throw new IllegalArgumentException("avoid using ZERO_UUID as a topicId because it is often used to represent unset");
            }
            if (topicNames.containsKey(topicId)) {
                throw new IllegalStateException("topic id " + topicId + " already has name " + topicNames.get(topicId));
            }
            topicNames.put(topicId, topicName);
            return this;
        }

        /**
         * Configures the SNI hostname reported by the context.
         *
         * @param sniHostname the SNI hostname, may be null
         * @return this builder
         */
        public MockFilterContextBuilder withSniHostname(@Nullable String sniHostname) {
            this.sniHostname = sniHostname;
            return this;
        }

        /**
         * Configures the session id reported by the context.
         *
         * @param sessionId the session id
         * @return this builder
         */
        public MockFilterContextBuilder withSessionId(String sessionId) {
            Objects.requireNonNull(sessionId);
            this.sessionId = sessionId;
            return this;
        }

        /**
         * Configures the channel descriptor reported by the context.
         *
         * @param channelDescriptor the channel descriptor
         * @return this builder
         */
        public MockFilterContextBuilder withChannelDescriptor(String channelDescriptor) {
            Objects.requireNonNull(channelDescriptor);
            this.channelDescriptor = channelDescriptor;
            return this;
        }

        /**
         * Configures the virtual cluster name reported by the context.
         *
         * @param virtualClusterName the virtual cluster name
         * @return this builder
         */
        public MockFilterContextBuilder withVirtualClusterName(String virtualClusterName) {
            Objects.requireNonNull(virtualClusterName);
            this.virtualClusterName = virtualClusterName;
            return this;
        }

        /**
         * Configures the context with a client SASL context with the given mechanism and authorization id.
         *
         * @param mechanismName the SASL mechanism name
         * @param authorizationId the authorization id
         * @return this builder
         */
        public MockFilterContextBuilder withClientSaslContext(String mechanismName, String authorizationId) {
            Objects.requireNonNull(mechanismName);
            Objects.requireNonNull(authorizationId);
            clientSaslContext = new MockClientSaslContext(mechanismName, authorizationId);
            return this;
        }

        /**
         * Configures the client SASL context reported by the context.
         *
         * @param clientSaslContext the client SASL context, may be null
         * @return this builder
         */
        public MockFilterContextBuilder withClientSaslContext(@Nullable ClientSaslContext clientSaslContext) {
            this.clientSaslContext = clientSaslContext;
            return this;
        }

        /**
         * Configures the context with a client TLS context with the given certificates.
         *
         * @param proxyServerCertificate the certificate presented by the proxy to the client
         * @param clientCertificate the certificate presented by the client, may be null
         * @return this builder
         */
        public MockFilterContextBuilder withClientTlsContext(X509Certificate proxyServerCertificate, @Nullable X509Certificate clientCertificate) {
            this.clientTlsContext = new MockClientTlsContext(proxyServerCertificate, clientCertificate);
            return this;
        }

        /**
         * Configures the client TLS context reported by the context.
         *
         * @param context the client TLS context
         * @return this builder
         */
        public MockFilterContextBuilder withClientTlsContext(ClientTlsContext context) {
            this.clientTlsContext = context;
            return this;
        }

        /**
         * Configures the authenticated subject reported by the context.
         *
         * @param subject the authenticated subject
         * @return this builder
         */
        public MockFilterContextBuilder withAuthenticatedSubject(Subject subject) {
            Objects.requireNonNull(subject);
            this.authenticatedSubject = subject;
            return this;
        }

        /**
         * Enqueues a response to be returned by the next invocation of {@link FilterContext#sendRequest(RequestHeaderData, ApiMessage)}.
         *
         * @param response the response to return
         * @return this builder
         */
        public MockFilterContextBuilder withSendRequestResponseEnqueued(ApiMessage response) {
            sendRequestResponses.add(new MockSendRequestResponse(header -> {
            }, body -> {
            }, response));
            return this;
        }

        /**
         * Enqueues a response to be returned by the next invocation of {@link FilterContext#sendRequest(RequestHeaderData, ApiMessage)},
         * validating that the header and request passed to sendRequest are equal to the expected ones.
         *
         * @param expectedHeader the header expected to be passed to sendRequest
         * @param expectedRequest the request expected to be passed to sendRequest
         * @param response the response to return
         * @return this builder
         */
        public MockFilterContextBuilder withSendRequestResponseEnqueued(ApiMessage expectedHeader, ApiMessage expectedRequest, ApiMessage response) {
            sendRequestResponses.add(new MockSendRequestResponse(header -> {
                if (!expectedHeader.equals(header)) {
                    throw new IllegalArgumentException("header being passed to sendRequest did not equal expected header: " + expectedHeader);
                }
            }, request -> {
                if (!expectedRequest.equals(request)) {
                    throw new IllegalArgumentException("request being passed to sendRequest did not equal expected request: " + expectedRequest);
                }
            }, response));
            return this;
        }

        /**
         * Configures a topic id to fail with the given exception when passed to {@link FilterContext#topicNames(Collection)}.
         *
         * @param topicId the topic id
         * @param exception the exception the mapping fails with
         * @return this builder
         */
        public MockFilterContextBuilder withTopicNameError(Uuid topicId, TopicNameMappingException exception) {
            Objects.requireNonNull(topicId);
            Objects.requireNonNull(exception);
            topicNameFailures.put(topicId, exception);
            return this;
        }

        private static class MockClientTlsContext implements ClientTlsContext {
            private final X509Certificate proxyServerCertificate;
            @Nullable
            private final X509Certificate clientCertificate;

            private MockClientTlsContext(X509Certificate proxyServerCertificate, @Nullable X509Certificate clientCertificate) {
                this.proxyServerCertificate = Objects.requireNonNull(proxyServerCertificate);
                this.clientCertificate = clientCertificate;
            }

            @Override
            public X509Certificate proxyServerCertificate() {
                return proxyServerCertificate;
            }

            @Override
            public Optional<X509Certificate> clientCertificate() {
                return Optional.ofNullable(clientCertificate);
            }
        }
    }

    private record MockClientSaslContext(String mechanismName, String authorizationId) implements ClientSaslContext {}

    @Override
    public String channelDescriptor() {
        return channelDescriptor;
    }

    @Override
    public String sessionId() {
        return sessionId;
    }

    @Override
    public ByteBufferOutputStream createByteBufferOutputStream(int initialCapacity) {
        return new ByteBufferOutputStream(initialCapacity);
    }

    @Nullable
    @Override
    public String sniHostname() {
        return sniHostname;
    }

    @Override
    public RequestFilterResultBuilder requestFilterResultBuilder() {
        return new MockRequestFilterResultBuilder(header, message);
    }

    @Override
    public CompletionStage<RequestFilterResult> forwardRequest(RequestHeaderData header, ApiMessage request) {
        return CompletableFuture.completedFuture(new MockRequestFilterResult(false, header, request, false, false));
    }

    /**
     * Returns the invocations of {@link FilterContext#sendRequest(RequestHeaderData, ApiMessage)} recorded by this context.
     *
     * @return the recorded invocations
     */
    public List<SendRequestInvocation> sendRequestInvocations() {
        return List.copyOf(sendRequestInvocations);
    }

    /**
     * Returns the client SASL authentication gestures recorded by this context.
     *
     * @return the recorded gestures
     */
    public List<ClientSaslGestureInvocation> clientSaslGestureInvocations() {
        return List.copyOf(clientSaslGestureInvocations);
    }

    @Override
    public <M extends ApiMessage> CompletionStage<M> sendRequest(RequestHeaderData header, ApiMessage request) {
        synchronized (sendRequestResponses) {
            sendRequestInvocations.add(new SendRequestInvocation(header, request));
            if (!sendRequestResponses.isEmpty()) {
                MockSendRequestResponse first = sendRequestResponses.remove(0);
                @SuppressWarnings("unchecked")
                M response = (M) first.respond(header, request);
                return CompletableFuture.completedFuture(response);
            }
            else {
                throw new IllegalStateException("Filter invoked sendRequest, but no responses enqueued");
            }
        }
    }

    @Override
    public CompletionStage<TopicNameMapping> topicNames(Collection<Uuid> topicIds) {
        Map<Boolean, List<Uuid>> partitioned = topicIds.stream().collect(Collectors.partitioningBy(topicNames::containsKey));
        Map<Uuid, TopicNameMappingException> errors = partitioned.get(false).stream()
                .collect(Collectors.toMap(topicId -> topicId, topicId -> topicNameFailures.getOrDefault(topicId, notConfiguredException())));
        Map<Uuid, String> names = partitioned.get(true).stream().collect(Collectors.toMap(topicId -> topicId, topicNames::get));
        return CompletableFuture.completedStage(
                new MockTopicNameMapping(!errors.isEmpty(), names, errors));
    }

    @Override
    public CompletionStage<ResponseFilterResult> forwardResponse(ResponseHeaderData header, ApiMessage response) {
        return CompletableFuture.completedFuture(new MockResponseFilterResult(false, header, response, false, false));
    }

    @Override
    public ResponseFilterResultBuilder responseFilterResultBuilder() {
        return new MockResponseFilterResultBuilder(header, message);
    }

    @Override
    public String getVirtualClusterName() {
        return virtualClusterName;
    }

    @Override
    public Optional<ClientTlsContext> clientTlsContext() {
        return Optional.ofNullable(clientTlsContext);
    }

    @Override
    public void clientSaslAuthenticationSuccess(String mechanism, Subject subject) {
        clientSaslGestureInvocations.add(new ClientSaslGestureInvocation.AuthenticationSuccess(mechanism, subject));
    }

    @Override
    public void clientSaslAuthenticationFailure(@Nullable String mechanism, @Nullable String authorizedId, Exception exception) {
        clientSaslGestureInvocations.add(new ClientSaslGestureInvocation.AuthenticationFailure(mechanism, authorizedId, exception));
    }

    @Override
    public Optional<ClientSaslContext> clientSaslContext() {
        return Optional.ofNullable(clientSaslContext);
    }

    @Override
    public Subject authenticatedSubject() {
        return authenticatedSubject;
    }

    private record MockRequestFilterResultBuilder(ApiMessage header, ApiMessage message) implements RequestFilterResultBuilder {
        @Override
        public CloseOrTerminalStage<RequestFilterResult> shortCircuitResponse(@Nullable ResponseHeaderData header, ApiMessage message)
                throws IllegalArgumentException {
            return new MockCloseOrTerminalRequestStage(true, header, message, false, false);
        }

        @Override
        public CloseOrTerminalStage<RequestFilterResult> shortCircuitResponse(ApiMessage message) throws IllegalArgumentException {
            return new MockCloseOrTerminalRequestStage(true, header, message, false, false);
        }

        @Override
        public CloseOrTerminalStage<RequestFilterResult> errorResponse(RequestHeaderData header, ApiMessage requestMessage, Errors error)
                throws IllegalArgumentException {
            return errorResponse(header, requestMessage, error, null);
        }

        @Override
        public CloseOrTerminalStage<RequestFilterResult> errorResponse(RequestHeaderData header, ApiMessage requestMessage, Errors error, @Nullable String message)
                throws IllegalArgumentException {
            Objects.requireNonNull(error, "error must not be null");
            if (error == Errors.NONE) {
                throw new IllegalArgumentException("error must denote an actual error, but was Errors.NONE");
            }
            return new MockErrorCloseOrTerminalRequestStage(header, requestMessage, error, message);
        }

        @Override
        public CloseOrTerminalStage<RequestFilterResult> forward(RequestHeaderData header, ApiMessage message) throws IllegalArgumentException {
            return new MockCloseOrTerminalRequestStage(false, header, message, false, false);
        }

        @Override
        public TerminalStage<RequestFilterResult> drop() {
            return new MockTerminalRequestStage(false, header, message, false, true);
        }

        @Override
        public TerminalStage<RequestFilterResult> withCloseConnection() {
            return new MockTerminalRequestStage(false, header, message, true, false);
        }
    }

    private record MockRequestFilterResult(boolean shortCircuitResponse,
                                           @Nullable ApiMessage header,
                                           @Nullable ApiMessage message,
                                           boolean closeConnection,
                                           boolean drop)
            implements RequestFilterResult {}

    /**
     * A RequestFilterResult capturing the inputs of an errorResponse invocation on the RequestFilterResultBuilder.
     *
     * @param header the request header passed to errorResponse
     * @param message the request message passed to errorResponse
     * @param error the error code passed to errorResponse
     * @param errorMessage the error message passed to errorResponse, or {@code null} to use the error's default message
     * @param closeConnection whether the connection should be closed
     */
    public record MockErrorRequestFilterResult(ApiMessage header,
                                               ApiMessage message,
                                               Errors error,
                                               @Nullable String errorMessage,
                                               boolean closeConnection)
            implements RequestFilterResult {
        @Override
        public boolean shortCircuitResponse() {
            return true;
        }

        @Override
        public boolean drop() {
            return false;
        }
    }

    private record MockResponseFilterResult(boolean shortCircuitResponse,
                                            @Nullable ApiMessage header,
                                            @Nullable ApiMessage message,
                                            boolean closeConnection,
                                            boolean drop)
            implements ResponseFilterResult {}

    private record MockResponseFilterResultBuilder(ApiMessage header, ApiMessage message) implements ResponseFilterResultBuilder {

        @Override
        public CloseOrTerminalStage<ResponseFilterResult> forward(ResponseHeaderData header, ApiMessage message) throws IllegalArgumentException {
            return new MockCloseTerminalResponseStage(header, message);
        }

        @Override
        public TerminalStage<ResponseFilterResult> drop() {
            return new MockTerminalResponseStage(false, header, message, false, true);
        }

        @Override
        public TerminalStage<ResponseFilterResult> withCloseConnection() {
            return new MockTerminalResponseStage(false, header, message, true, false);
        }
    }

    private record MockTerminalRequestStage(boolean shortCircuitResponse,
                                            @Nullable ApiMessage header,
                                            @Nullable ApiMessage message,
                                            boolean closeConnection,
                                            boolean drop)
            implements TerminalStage<RequestFilterResult> {

        @Override
        public RequestFilterResult build() {
            return new MockRequestFilterResult(shortCircuitResponse, header, message, closeConnection, drop);
        }

        @Override
        public CompletionStage<RequestFilterResult> completed() {
            return CompletableFuture.completedFuture(build());
        }
    }

    private record MockErrorTerminalRequestStage(RequestHeaderData header,
                                                 ApiMessage requestMessage,
                                                 Errors error,
                                                 @Nullable String errorMessage,
                                                 boolean closeConnection)
            implements TerminalStage<RequestFilterResult> {

        @Override
        public RequestFilterResult build() {
            return new MockErrorRequestFilterResult(header, requestMessage, error, errorMessage, closeConnection);
        }

        @Override
        public CompletionStage<RequestFilterResult> completed() {
            return CompletableFuture.completedFuture(build());
        }
    }

    private record MockErrorCloseOrTerminalRequestStage(RequestHeaderData header,
                                                        ApiMessage requestMessage,
                                                        Errors error,
                                                        @Nullable String errorMessage)
            implements CloseOrTerminalStage<RequestFilterResult> {

        @Override
        public RequestFilterResult build() {
            return new MockErrorRequestFilterResult(header, requestMessage, error, errorMessage, false);
        }

        @Override
        public CompletionStage<RequestFilterResult> completed() {
            return CompletableFuture.completedFuture(build());
        }

        @Override
        public TerminalStage<RequestFilterResult> withCloseConnection() {
            return new MockErrorTerminalRequestStage(header, requestMessage, error, errorMessage, true);
        }
    }

    private record MockCloseOrTerminalRequestStage(boolean shortCircuitResponse,
                                                   @Nullable ApiMessage header,
                                                   @Nullable ApiMessage message,
                                                   boolean closeConnection,
                                                   boolean drop)
            implements CloseOrTerminalStage<RequestFilterResult> {

        @Override
        public RequestFilterResult build() {
            return new MockRequestFilterResult(shortCircuitResponse, header, message, closeConnection, drop);
        }

        @Override
        public CompletionStage<RequestFilterResult> completed() {
            return CompletableFuture.completedFuture(build());
        }

        @Override
        public TerminalStage<RequestFilterResult> withCloseConnection() {
            return new MockTerminalRequestStage(shortCircuitResponse, header, message, true, drop);
        }
    }

    private record MockTerminalResponseStage(boolean shortCircuitResponse,
                                             @Nullable ApiMessage header,
                                             @Nullable ApiMessage message,
                                             boolean closeConnection,
                                             boolean drop)
            implements TerminalStage<ResponseFilterResult> {

        @Override
        public ResponseFilterResult build() {
            return new MockResponseFilterResult(shortCircuitResponse, header, message, closeConnection, drop);
        }

        @Override
        public CompletionStage<ResponseFilterResult> completed() {
            return CompletableFuture.completedFuture(build());
        }
    }

    private record MockCloseTerminalResponseStage(@Nullable ApiMessage header,
                                                  @Nullable ApiMessage message)
            implements CloseOrTerminalStage<ResponseFilterResult> {

        @Override
        public ResponseFilterResult build() {
            return new MockResponseFilterResult(false, header, message, false, false);
        }

        @Override
        public CompletionStage<ResponseFilterResult> completed() {
            return CompletableFuture.completedFuture(build());
        }

        @Override
        public TerminalStage<ResponseFilterResult> withCloseConnection() {
            return new MockTerminalResponseStage(false, header, message, true, false);
        }
    }

    private record MockTopicNameMapping(boolean anyFailures, Map<Uuid, String> topicNames, Map<Uuid, TopicNameMappingException> failures) implements TopicNameMapping {

    }

    private record MockSendRequestResponse(Consumer<ApiMessage> headerValidator, Consumer<ApiMessage> bodyValidator, ApiMessage response) {
        ApiMessage respond(ApiMessage requestHeader, ApiMessage requestBody) {
            headerValidator.accept(requestHeader);
            bodyValidator.accept(requestBody);
            return response;
        }
    }

    /**
     * A recorded invocation of {@link FilterContext#sendRequest(RequestHeaderData, ApiMessage)}.
     *
     * @param header the header passed to sendRequest
     * @param request the request passed to sendRequest
     */
    public record SendRequestInvocation(ApiMessage header, ApiMessage request) {

    }

    /**
     * A recorded client SASL authentication gesture.
     */
    public sealed interface ClientSaslGestureInvocation {
        /**
         * A recorded invocation of {@link FilterContext#clientSaslAuthenticationSuccess(String, Subject)}.
         *
         * @param mechanism the SASL mechanism
         * @param subject the authenticated subject
         */
        record AuthenticationSuccess(String mechanism, Subject subject) implements ClientSaslGestureInvocation {}

        /**
         * A recorded invocation of {@link FilterContext#clientSaslAuthenticationFailure(String, String, Exception)}.
         *
         * @param mechanism the SASL mechanism, may be null
         * @param authorizedId the authorization id, may be null
         * @param exception the exception describing the failure
         */
        record AuthenticationFailure(@Nullable String mechanism, @Nullable String authorizedId, Exception exception) implements ClientSaslGestureInvocation {}
    }
}
