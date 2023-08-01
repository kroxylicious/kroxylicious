/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.CloseStage;
import io.kroxylicious.proxy.filter.FilterResult;
import io.kroxylicious.proxy.filter.FilterResultBuilder;
import io.kroxylicious.proxy.filter.TerminalStage;

public abstract class FilterResultBuilderImpl<H extends ApiMessage, FRB extends FilterResultBuilder<H, FRB, FR>, FR extends FilterResult>
        implements FilterResultBuilder<H, FRB, FR>, CloseStage<FR> {
    private ApiMessage message;
    private ApiMessage header;
    private boolean closeConnection;
    private boolean drop;

    protected FilterResultBuilderImpl() {
    }

    @Override
    public CloseStage<FR> forward(H header, ApiMessage message) {
        validateForward(header, message);
        this.header = header;
        this.message = message;

        return this;
    }

    protected void validateForward(H header, ApiMessage message) {
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

    @Override
    public TerminalStage<FR> withCloseConnection2(boolean closeConnection) {
        this.closeConnection = closeConnection;
        return this;
    }

    boolean closeConnection() {
        return closeConnection;
    }

    @Override
    public TerminalStage<FR> drop() {
        this.drop = true;
        return this;
    }

    public boolean isDrop() {
        return drop;
    }

    @Override
    public CompletionStage<FR> completedFilterResult() {
        return CompletableFuture.completedStage(build());
    }
}
