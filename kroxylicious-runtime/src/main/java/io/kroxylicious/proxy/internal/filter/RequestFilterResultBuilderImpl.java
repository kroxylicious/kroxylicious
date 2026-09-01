/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.Objects;

import org.apache.kafka.common.errors.ApiException;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;

import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.internal.KafkaProxyExceptionMapper;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Builder of {@link RequestFilterResult} instances. In addition to forwarding, supports
 * short-circuit responses (answering the client without forwarding the request upstream),
 * including error responses derived from an {@link ApiException}.
 */
public class RequestFilterResultBuilderImpl extends FilterResultBuilderImpl<RequestHeaderData, RequestFilterResult>
        implements RequestFilterResultBuilder {

    private static final String REQUEST_DATA_NAME_SUFFIX = "RequestData";
    private static final String RESPONSE_DATA_NAME_SUFFIX = "ResponseData";
    private @Nullable ResponseHeaderData shortCircuitHeader;
    private @Nullable ApiMessage shortCircuitResponse;

    /**
     * Creates an empty builder.
     */
    public RequestFilterResultBuilderImpl() {
        // Intentionally empty
    }

    @Override
    protected void validateForward(RequestHeaderData header, ApiMessage message) {
        super.validateForward(header, message);
        if (!message.getClass().getSimpleName().endsWith(REQUEST_DATA_NAME_SUFFIX)) {
            throw new IllegalArgumentException("class name " + message.getClass().getName() + " does not have expected suffix " + REQUEST_DATA_NAME_SUFFIX);
        }
    }

    @Override
    public CloseOrTerminalStage<RequestFilterResult> shortCircuitResponse(@Nullable ResponseHeaderData header, ApiMessage message) {
        validateShortCircuitResponse(message);
        this.shortCircuitHeader = header;
        this.shortCircuitResponse = message;
        return this;
    }

    @Override
    public CloseOrTerminalStage<RequestFilterResult> shortCircuitResponse(ApiMessage message) {
        validateShortCircuitResponse(message);
        this.shortCircuitResponse = message;
        return this;
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
        return errorResponseForException(header, requestMessage, error, message);
    }

    private CloseOrTerminalStage<RequestFilterResult> errorResponseForException(RequestHeaderData header, ApiMessage requestMessage, Errors error,
                                                                                @Nullable String message) {
        ApiKeys apiKey = ApiKeys.forId(requestMessage.apiKey());
        final ApiMessage errorResponseMessage = KafkaProxyExceptionMapper.errorResponseData(apiKey, requestMessage, header.requestApiVersion(), error, message);
        validateShortCircuitResponse(errorResponseMessage);
        final ResponseHeaderData responseHeaders = new ResponseHeaderData();
        responseHeaders.setCorrelationId(header.correlationId());
        this.shortCircuitHeader = responseHeaders;
        this.shortCircuitResponse = errorResponseMessage;
        return this;
    }

    private void validateShortCircuitResponse(@Nullable ApiMessage message) {
        if (message == null) {
            throw new IllegalArgumentException("message may not be null");
        }
        if (!message.getClass().getSimpleName().endsWith(RESPONSE_DATA_NAME_SUFFIX)) {
            throw new IllegalArgumentException("class name " + message.getClass().getName() + " does not have expected suffix " + RESPONSE_DATA_NAME_SUFFIX);
        }
    }

    @Override
    public RequestFilterResult build() {

        return new RequestFilterResult() {

            @Override
            public boolean shortCircuitResponse() {
                return shortCircuitResponse != null;
            }

            @Override
            public @Nullable ApiMessage header() {
                return shortCircuitResponse == null ? RequestFilterResultBuilderImpl.this.header() : shortCircuitHeader;
            }

            @Override
            public @Nullable ApiMessage message() {
                return shortCircuitResponse == null ? RequestFilterResultBuilderImpl.this.message() : shortCircuitResponse;
            }

            @Override
            public boolean closeConnection() {
                return RequestFilterResultBuilderImpl.this.closeConnection();
            }

            @Override
            public boolean drop() {
                return RequestFilterResultBuilderImpl.this.isDrop();
            }
        };

    }
}
