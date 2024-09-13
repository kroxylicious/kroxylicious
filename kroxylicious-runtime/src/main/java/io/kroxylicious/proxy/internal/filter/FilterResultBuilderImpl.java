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
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage;

import edu.umd.cs.findbugs.annotations.NonNull;

public abstract class FilterResultBuilderImpl<H extends ApiMessage, R extends FilterResult>
                                             implements FilterResultBuilder<H, R>, CloseOrTerminalStage<R> {
    private ApiMessage message;
    private ApiMessage header;
    private boolean closeConnection;
    private boolean drop;

    protected FilterResultBuilderImpl() {
    }

    @Override
    public CloseOrTerminalStage<R> forward(
            @NonNull
            H header,
            @NonNull
            ApiMessage message
    ) {
        validateForward(header, message);
        this.header = header;
        this.message = message;

        return this;
    }

    protected void validateForward(H header, ApiMessage message) {
        if (header == null) {
            throw new IllegalArgumentException("header may not be null");
        }
        if (message == null) {
            throw new IllegalArgumentException("message may not be null");
        }
    }

    ApiMessage header() {
        return header;
    }

    ApiMessage message() {
        return message;
    }

    @Override
    public TerminalStage<R> withCloseConnection() {
        this.closeConnection = true;
        return this;
    }

    boolean closeConnection() {
        return closeConnection;
    }

    @Override
    public TerminalStage<R> drop() {
        this.drop = true;
        return this;
    }

    public boolean isDrop() {
        return drop;
    }

    @Override
    public CompletionStage<R> completed() {
        return CompletableFuture.completedStage(build());
    }
}
