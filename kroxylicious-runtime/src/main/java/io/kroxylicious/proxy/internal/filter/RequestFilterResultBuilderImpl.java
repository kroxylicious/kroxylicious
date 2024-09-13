/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class RequestFilterResultBuilderImpl extends FilterResultBuilderImpl<RequestHeaderData, RequestFilterResult>
                                            implements RequestFilterResultBuilder {

    private static final String REQUEST_DATA_NAME_SUFFIX = "RequestData";
    private static final String RESPONSE_DATA_NAME_SUFFIX = "ResponseData";
    private ResponseHeaderData shortCircuitHeader;
    private ApiMessage shortCircuitResponse;

    @Override
    protected void validateForward(RequestHeaderData header, ApiMessage message) {
        super.validateForward(header, message);
        if (message != null && !message.getClass().getSimpleName().endsWith(REQUEST_DATA_NAME_SUFFIX)) {
            throw new IllegalArgumentException("class name " + message.getClass().getName() + " does not have expected suffix " + REQUEST_DATA_NAME_SUFFIX);
        }
    }

    @Override
    public CloseOrTerminalStage<RequestFilterResult> shortCircuitResponse(
            @Nullable
            ResponseHeaderData header,
            @NonNull
            ApiMessage message
    ) {
        validateShortCircuitResponse(message);
        this.shortCircuitHeader = header;
        this.shortCircuitResponse = message;
        return this;
    }

    @Override
    public CloseOrTerminalStage<RequestFilterResult> shortCircuitResponse(
            @NonNull
            ApiMessage message
    ) {
        validateShortCircuitResponse(message);
        this.shortCircuitResponse = message;
        return this;
    }

    private void validateShortCircuitResponse(ApiMessage message) {
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
            public ApiMessage header() {

                return shortCircuitResponse == null ? RequestFilterResultBuilderImpl.this.header() : shortCircuitHeader;
            }

            @Override
            public ApiMessage message() {
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
