/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterResult;
import io.kroxylicious.proxy.filter.FilterResultBuilder;

public abstract class FilterResultBuilderImpl<FRB extends FilterResultBuilder<FRB, FR>, FR extends FilterResult>
        implements FilterResultBuilder<FRB, FR> {
    private ApiMessage message;
    private ApiMessage header;
    private boolean closeConnection;

    protected FilterResultBuilderImpl() {
    }

    @Override
    public FRB withHeader(ApiMessage header) {
        validateHeader(header);
        this.header = header;
        return (FRB) this;
    }

    protected void validateHeader(ApiMessage header) {
    }

    ApiMessage header() {
        return header;
    }

    @Override
    public FRB withMessage(ApiMessage message) {
        validateMessage(message);
        this.message = message;
        return (FRB) this;
    }

    protected void validateMessage(ApiMessage message) {
    }

    ApiMessage message() {
        return message;
    }

    @Override
    public FRB withCloseConnection(boolean closeConnection) {
        this.closeConnection = closeConnection;
        return (FRB) this;
    }

    boolean closeConnection() {
        return closeConnection;
    }

    @Override
    public CompletionStage<FR> completedFilterResult() {
        return CompletableFuture.completedStage(build());
    }
}