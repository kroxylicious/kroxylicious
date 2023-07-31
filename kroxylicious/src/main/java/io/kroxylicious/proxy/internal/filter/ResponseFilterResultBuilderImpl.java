/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResultBuilder;

public class ResponseFilterResultBuilderImpl extends FilterResultBuilderImpl<ResponseFilterResultBuilder, ResponseFilterResult>
        implements ResponseFilterResultBuilder {

    private static final String RESPONSE_DATA_NAME_SUFFIX = "ResponseData";

    @Override
    protected void validateMessage(ApiMessage message) {
        super.validateMessage(message);
        if (message != null && !message.getClass().getSimpleName().endsWith(RESPONSE_DATA_NAME_SUFFIX)) {
            throw new IllegalArgumentException("class name " + message.getClass().getName() + " does not have expected suffix " + RESPONSE_DATA_NAME_SUFFIX);
        }
    }

    @Override
    protected void validateHeader(ApiMessage header) {
        super.validateHeader(header);
        if (header != null && !(header instanceof ResponseHeaderData)) {
            throw new IllegalArgumentException("header " + header.getClass().getName() + " does not implement expected class " + RequestHeaderData.class.getName());
        }
    }

    @Override
    public ResponseFilterResult build() {
        return new ResponseFilterResult() {
            @Override
            public ApiMessage header() {
                return ResponseFilterResultBuilderImpl.this.header();
            }

            @Override
            public ApiMessage message() {
                return ResponseFilterResultBuilderImpl.this.message();
            }

            @Override
            public boolean closeConnection() {
                return ResponseFilterResultBuilderImpl.this.closeConnection();
            }
        };
    }

}
