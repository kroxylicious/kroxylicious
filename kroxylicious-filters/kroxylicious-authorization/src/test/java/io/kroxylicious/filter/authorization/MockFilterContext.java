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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import io.kroxylicious.proxy.audit.AuditLogger;
import io.kroxylicious.proxy.audit.AuditableActionBuilder;
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

public record MockFilterContext(ApiMessage header, ApiMessage message, Subject subject, Map<Uuid, String> topicNames, MockUpstream mockUpstream,
                                MockAuditLogger mockAuditLogger)
        implements FilterContext {

    public MockFilterContext {
        Objects.requireNonNull(subject, "Subject cannot be null");
    }

    public MockFilterContext(ApiMessage header, ApiMessage message, Subject subject, Map<Uuid, String> topicNames, MockUpstream mockUpstream) {
        this(header, message, subject, topicNames, mockUpstream, new MockAuditLogger());
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
        return CompletableFuture.completedFuture(new MockResponseFilterResult(header, response, false, false));
    }

    @NonNull
    @Override
    public ResponseFilterResultBuilder responseFilterResultBuilder() {
        throw new UnsupportedOperationException();
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
    public void clientSaslAuthenticationSuccess(@NonNull String mechanism, @NonNull String authorizedId) {
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

    @Override
    public AuditLogger auditLogger() {
        return mockAuditLogger;
    }

    record MockRequestFilterResult(boolean shortCircuitResponse,
                                   @Nullable ApiMessage header,
                                   @Nullable ApiMessage message,
                                   boolean closeConnection,
                                   boolean drop)
            implements RequestFilterResult {}

    record MockResponseFilterResult(@Nullable ApiMessage header,
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
        public CloseOrTerminalStage<RequestFilterResult> errorResponse(@NonNull RequestHeaderData header, @NonNull ApiMessage requestMessage,
                                                                       @NonNull ApiException apiException)
                throws IllegalArgumentException {
            return new ErrorCloseOrTerminalStage(header, requestMessage, apiException, false);
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

        private record ErrorCloseOrTerminalStage(RequestHeaderData header, ApiMessage message, ApiException apiException, boolean closeConnection)
                implements CloseOrTerminalStage<RequestFilterResult> {
            @Override
            public TerminalStage<RequestFilterResult> withCloseConnection() {
                return new ErrorCloseOrTerminalStage(header, message, apiException, true);
            }

            @Override
            public RequestFilterResult build() {
                return new ErrorRequestFilterResult(header, message, apiException, closeConnection);
            }

            @Override
            public CompletionStage<RequestFilterResult> completed() {
                return CompletableFuture.completedFuture(build());
            }

        }
    }

    record ErrorRequestFilterResult(RequestHeaderData header, ApiMessage message, ApiException apiException, boolean closeConnection)
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

    record AuditEvent(
                      String action,
                      @Nullable String status,
                      @Nullable String reason,
                      Map<String, String> objectRef,
                      @Nullable Map<String, Object> context) {}

    public static class MockAuditLogger implements AuditLogger<AuditableActionBuilder> {
        private final List<AuditEvent> events = new java.util.ArrayList<>();

        public List<AuditEvent> getEvents() {
            return java.util.Collections.unmodifiableList(events);
        }

        @Override
        public AuditableActionBuilder action(String action) {
            return new MockAuditableActionBuilder(action, null, null, events);
        }

        @Override
        public AuditableActionBuilder actionWithOutcome(String action, String status, @Nullable String reason) {
            return new MockAuditableActionBuilder(action, status, reason, events);
        }

    }

    static class MockAuditableActionBuilder implements AuditableActionBuilder {
        private final String action;
        private final String status;
        private final String reason;
        private final List<AuditEvent> events;
        private Map<String, String> objectRef;
        private Map<String, Object> context = new java.util.HashMap<>();

        MockAuditableActionBuilder(String action, String status, String reason, List<AuditEvent> events) {
            this.action = action;
            this.status = status;
            this.reason = reason;
            this.events = events;
        }

        @Override
        public AuditableActionBuilder withObjectRef(Map<String, String> objectRef) {
            this.objectRef = objectRef;
            return this;
        }

        @Override
        public AuditableActionBuilder addToContext(String key, boolean value) {
            this.context.put(key, value);
            return this;
        }

        @Override
        public AuditableActionBuilder addToContext(String key, long value) {
            this.context.put(key, value);
            return this;
        }

        @Override
        public AuditableActionBuilder addToContext(String key, double value) {
            this.context.put(key, value);
            return this;
        }

        @Override
        public AuditableActionBuilder addToContext(String key, String value) {
            this.context.put(key, value);
            return this;
        }

        @Override
        public AuditableActionBuilder addToContext(String key, boolean[] value) {
            this.context.put(key, value);
            return this;
        }

        @Override
        public AuditableActionBuilder addToContext(String key, long[] value) {
            this.context.put(key, value);
            return this;
        }

        @Override
        public AuditableActionBuilder addToContext(String key, double[] value) {
            this.context.put(key, value);
            return this;
        }

        @Override
        public AuditableActionBuilder addToContext(String key, String[] value) {
            this.context.put(key, value);
            return this;
        }

        @Override
        public void log() {
            events.add(new AuditEvent(action, status, reason, objectRef, context));
        }
    }
}
