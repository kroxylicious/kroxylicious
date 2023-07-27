/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

public abstract class FilterResultBuilder<M, H> {
    private ApiMessage message;
    private H header;
    private boolean closeConnection;

    private FilterResultBuilder() {
    }

    public FilterResultBuilder<M, H> withHeader(H header) {
        this.header = header;
        return this;
    }

    H header() {
        return header;
    }

    public FilterResultBuilder<M, H> withMessage(ApiMessage message) {
        this.message = message;
        return this;
    }

    ApiMessage message() {
        return message;
    }

    public FilterResultBuilder<M, H> withCloseConnection(boolean closeConnection) {
        this.closeConnection = closeConnection;
        return this;
    }

    boolean closeConnection() {
        return closeConnection;
    }

    public abstract M build();

    public static FilterResultBuilder<ResponseFilterResult, ResponseHeaderData> responseFilterResultBuilder() {
        return new FilterResultBuilder<>() {
            @Override
            public ResponseFilterResult build() {
                var builderThis = this;
                return new ResponseFilterResult() {
                    @Override
                    public ResponseHeaderData header() {
                        return builderThis.header();
                    }

                    @Override
                    public ApiMessage message() {
                        return builderThis.message();
                    }

                    @Override
                    public boolean closeConnection() {
                        return builderThis.closeConnection();
                    }
                };
            }
        };
    }

    public static FilterResultBuilder<RequestFilterResult, RequestHeaderData> requestFilterResultBuilder() {
        return new FilterResultBuilder<>() {
            @Override
            public RequestFilterResult build() {

                var builder = this;
                return new RequestFilterResult() {
                    @Override
                    public RequestHeaderData header() {
                        return builder.header();
                    }

                    @Override
                    public ApiMessage message() {
                        return builder.message();
                    }

                    @Override
                    public boolean closeConnection() {
                        return builder.closeConnection();
                    }
                };
            }
        };
    }

}