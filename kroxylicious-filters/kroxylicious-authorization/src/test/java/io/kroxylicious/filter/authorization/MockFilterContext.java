/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

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
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResultBuilder;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public record MockFilterContext(ApiMessage header, ApiMessage message, Subject subject, Map<Uuid, String> topicNames, MockUpstream mockUpstream)
        implements FilterContext {

    public MockFilterContext {
        Objects.requireNonNull(subject, "Subject cannot be null");
    }

    @NonNull
    @Override
    public String channelDescriptor() {
        throw new UnsupportedOperationException();
    }

    @NonNull
    @Override
    public String sessionId() {
        return "mockSessionId";
    }

    @NonNull
    @Override
    public ByteBufferOutputStream createByteBufferOutputStream(int initialCapacity) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public String sniHostname() {
        throw new UnsupportedOperationException();
    }

    @NonNull
    @Override
    public io.kroxylicious.proxy.filter.RequestFilterResultBuilder requestFilterResultBuilder() {
        return new RequestFilterResultBuilder((RequestHeaderData) header, message);
    }

    @NonNull
    @Override
    public CompletionStage<RequestFilterResult> forwardRequest(@NonNull RequestHeaderData header, @NonNull ApiMessage request) {
        return CompletableFuture.completedFuture(new MockRequestFilterResult(false, header, request, false, false));
    }

    @NonNull
    @Override
    public <M extends ApiMessage> CompletionStage<M> sendRequest(@NonNull RequestHeaderData header, @NonNull ApiMessage request) {
        return mockUpstream.sendRequest(header, request);
    }

    @NonNull
    @Override
    public CompletionStage<TopicNameMapping> topicNames(Collection<Uuid> topicIds) {
        Map<Boolean, List<Uuid>> hasName = topicIds.stream().collect(Collectors.partitioningBy(topicNames::containsKey));
        List<Uuid> haveNames = hasName.get(true);
        List<Uuid> noNames = hasName.get(false);
        Map<Uuid, String> haveNamesMap = haveNames.stream().collect(Collectors.toMap(topic -> topic, topicNames::get));
        Map<Uuid, TopicNameMappingException> noNamesMap = noNames.stream()
                .collect(Collectors.toMap(topic -> topic, topic -> new TopicNameMappingException(Errors.UNKNOWN_SERVER_ERROR)));
        return CompletableFuture.completedFuture(new TopicNameMapping() {
            @Override
            public boolean anyFailures() {
                return !noNames.isEmpty();
            }

            @Override
            public Map<Uuid, String> topicNames() {
                return haveNamesMap;
            }

            @Override
            public Map<Uuid, TopicNameMappingException> failures() {
                return noNamesMap;
            }
        });
    }

    @NonNull
    @Override
    public CompletionStage<ResponseFilterResult> forwardResponse(@NonNull ResponseHeaderData header, @NonNull ApiMessage response) {
        return CompletableFuture.completedFuture(new MockResponseFilterResult(false, header, response, false, false));
    }

    @NonNull
    @Override
    public ResponseFilterResultBuilder responseFilterResultBuilder() {
        return new MockResponseFilterResultBuilder(header, message);
    }

    @NonNull
    @Override
    public String getVirtualClusterName() {
        throw new UnsupportedOperationException();
    }

    @NonNull
    @Override
    public Optional<ClientTlsContext> clientTlsContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clientSaslAuthenticationSuccess(@NonNull String mechanism, @NonNull Subject subject) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clientSaslAuthenticationFailure(@Nullable String mechanism, @Nullable String authorizedId, @NonNull Exception exception) {
        throw new UnsupportedOperationException();
    }

    @NonNull
    @Override
    public Optional<ClientSaslContext> clientSaslContext() {
        throw new UnsupportedOperationException();
    }

    @NonNull
    @Override
    public Subject authenticatedSubject() {
        return subject;
    }

    record MockRequestFilterResult(boolean shortCircuitResponse,
                                   @Nullable ApiMessage header,
                                   @Nullable ApiMessage message,
                                   boolean closeConnection,
                                   boolean drop)
            implements RequestFilterResult {}

    record MockResponseFilterResult(boolean shortCircuitResponse,
                                    @Nullable ApiMessage header,
                                    @Nullable ApiMessage message,
                                    boolean closeConnection,
                                    boolean drop)
            implements ResponseFilterResult {}

    record RequestTerminalStage(MockRequestFilterResult result) implements TerminalStage<RequestFilterResult> {

        @NonNull
        @Override
        public RequestFilterResult build() {
            return result;
        }

        @NonNull
        @Override
        public CompletionStage<RequestFilterResult> completed() {
            return CompletableFuture.completedFuture(result);
        }
    }

    record RequestCloseOrTerminalStage(MockRequestFilterResult result) implements CloseOrTerminalStage<RequestFilterResult> {

        @NonNull
        @Override
        public TerminalStage<RequestFilterResult> withCloseConnection() {
            return new RequestTerminalStage(new MockRequestFilterResult(result.shortCircuitResponse, result().header, result().message, true, result.drop()));
        }

        @NonNull
        @Override
        public RequestFilterResult build() {
            return result;
        }

        @NonNull
        @Override
        public CompletionStage<RequestFilterResult> completed() {
            return CompletableFuture.completedFuture(result);
        }
    }

    record RequestFilterResultBuilder(RequestHeaderData header, ApiMessage message) implements io.kroxylicious.proxy.filter.RequestFilterResultBuilder {

        @NonNull
        @Override
        public CloseOrTerminalStage<RequestFilterResult> shortCircuitResponse(@Nullable ResponseHeaderData header, @NonNull ApiMessage message)
                throws IllegalArgumentException {
            return new RequestCloseOrTerminalStage(new MockRequestFilterResult(true, header, message, false, false));
        }

        @NonNull
        @Override
        public CloseOrTerminalStage<RequestFilterResult> shortCircuitResponse(@NonNull ApiMessage message) throws IllegalArgumentException {
            ResponseHeaderData respo = new ResponseHeaderData();
            respo.setCorrelationId(header.correlationId());
            return new RequestCloseOrTerminalStage(new MockRequestFilterResult(true, respo, message, false, false));
        }

        @NonNull
        @Override
        public CloseOrTerminalStage<RequestFilterResult> errorResponse(@NonNull RequestHeaderData header, @NonNull ApiMessage requestMessage, @NonNull Errors error)
                throws IllegalArgumentException {
            return errorResponse(header, requestMessage, error, null);
        }

        @NonNull
        @Override
        public CloseOrTerminalStage<RequestFilterResult> errorResponse(@NonNull RequestHeaderData header, @NonNull ApiMessage requestMessage, @NonNull Errors error,
                                                                       @Nullable String message)
                throws IllegalArgumentException {
            Objects.requireNonNull(error, "error must not be null");
            if (error == Errors.NONE) {
                throw new IllegalArgumentException("error must denote an actual error, but was Errors.NONE");
            }
            // Errors.exception(String) returns the default-message exception when message is null.
            return new ErrorCloseOrTerminalStage(header, requestMessage, error, false);
        }

        @NonNull
        @Override
        public CloseOrTerminalStage<RequestFilterResult> forward(@NonNull RequestHeaderData header, @NonNull ApiMessage message) throws IllegalArgumentException {
            return new RequestCloseOrTerminalStage(new MockRequestFilterResult(false, header, message, false, false));
        }

        @NonNull
        @Override
        public TerminalStage<RequestFilterResult> drop() {
            return new RequestTerminalStage(new MockRequestFilterResult(false, null, null, false, true));
        }

        @NonNull
        @Override
        public TerminalStage<RequestFilterResult> withCloseConnection() {
            return new RequestTerminalStage(new MockRequestFilterResult(false, null, null, true, false));
        }

        private record ErrorCloseOrTerminalStage(RequestHeaderData header, ApiMessage message, Errors errors, boolean closeConnection)
                implements CloseOrTerminalStage<RequestFilterResult> {
            @Override
            public TerminalStage<RequestFilterResult> withCloseConnection() {
                return new ErrorCloseOrTerminalStage(header, message, errors, true);
            }

            @Override
            public RequestFilterResult build() {
                return new ErrorRequestFilterResult(header, message, errors, closeConnection);
            }

            @Override
            public CompletionStage<RequestFilterResult> completed() {
                return CompletableFuture.completedFuture(build());
            }

        }
    }

    record ErrorRequestFilterResult(RequestHeaderData header, ApiMessage message, Errors error, boolean closeConnection)
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
            return new MockFilterContext.MockErrorRequestFilterResult(header, requestMessage, error, errorMessage, false);
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

}
