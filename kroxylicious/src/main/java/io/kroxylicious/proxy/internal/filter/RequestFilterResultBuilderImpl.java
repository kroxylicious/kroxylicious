/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.CloseStage;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;

public class RequestFilterResultBuilderImpl extends FilterResultBuilderImpl<RequestHeaderData, RequestFilterResultBuilder, RequestFilterResult>
        implements RequestFilterResultBuilder {

    private static final String REQUEST_DATA_NAME_SUFFIX = "RequestData";
    private static final String RESPONSE_DATA_NAME_SUFFIX = "ResponseData";
    private boolean shortCircuitResponse;
    private ResponseHeaderData shortCircuitHeader;
    private ApiMessage shortCircuitResponse2;

    public RequestFilterResultBuilderImpl() {
    }

    @Override
    protected void validateForward(RequestHeaderData header, ApiMessage message) {
        super.validateForward(header, message);
        if (message != null && !message.getClass().getSimpleName().endsWith(REQUEST_DATA_NAME_SUFFIX)) {
            throw new IllegalArgumentException("class name " + message.getClass().getName() + " does not have expected suffix " + REQUEST_DATA_NAME_SUFFIX);
        }
    }

    @Override
    protected void validateMessage(ApiMessage message) {
        super.validateMessage(message);
        var expectedClassNameSuffix = shortCircuitResponse ? RESPONSE_DATA_NAME_SUFFIX : REQUEST_DATA_NAME_SUFFIX;
        if (message != null && !message.getClass().getSimpleName().endsWith(expectedClassNameSuffix)) {
            throw new IllegalArgumentException("class name " + message.getClass().getName() + " does not have expected suffix " + expectedClassNameSuffix);
        }
    }

    @Override
    protected void validateHeader(ApiMessage header) {
        super.validateHeader(header);
        var expectedInterface = shortCircuitResponse ? ResponseHeaderData.class : RequestHeaderData.class;
        if (header != null && !expectedInterface.isInstance(header)) {
            throw new IllegalArgumentException("header " + header.getClass().getName() + " does not implement expected class " + ResponseHeaderData.class.getName());

        }
    }

    @Override
    public CloseStage<RequestFilterResult> shortCircuitResponse(ResponseHeaderData header, ApiMessage message) {
        validateShortCircuitResponse(header, message);
        this.shortCircuitHeader = header;
        this.shortCircuitResponse2 = message;
        return this;
    }

    @Override
    public CloseStage<RequestFilterResult> shortCircuitResponse(ApiMessage message) {
        validateShortCircuitResponse(null, message);
        this.shortCircuitResponse2 = message;
        return this;
    }

    private void validateShortCircuitResponse(ResponseHeaderData header, ApiMessage message) {
        if (message != null && !message.getClass().getSimpleName().endsWith(RESPONSE_DATA_NAME_SUFFIX)) {
            throw new IllegalArgumentException("class name " + message.getClass().getName() + " does not have expected suffix " + RESPONSE_DATA_NAME_SUFFIX);
        }
    }

    @Override
    public RequestFilterResultBuilder asRequestShortCircuitResponse() {
        if (this.message() != null) {
            throw new IllegalStateException("cannot call asRequestShortCircuitResponse after message has been assigned");
        }
        if (this.header() != null) {
            throw new IllegalStateException("cannot call asRequestShortCircuitResponse after header has been assigned");
        }
        this.shortCircuitResponse = true;
        return this;
    }

    @Override
    public RequestFilterResult build() {

        return new RequestFilterResult() {

            @Override
            public boolean shortCircuitResponse() {
                return shortCircuitResponse2 != null;
            }

            @Override
            public ApiMessage header() {

                return shortCircuitResponse2 == null ? RequestFilterResultBuilderImpl.this.header() : shortCircuitHeader;
            }

            @Override
            public ApiMessage message() {
                return shortCircuitResponse2 == null ? RequestFilterResultBuilderImpl.this.message() : shortCircuitResponse2;
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
