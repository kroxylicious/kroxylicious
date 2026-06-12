/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.router.CloseOrTerminalStage;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.router.TerminalStage;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Builder that implements the router result stage interfaces.
 */
class RouterResultBuilderImpl implements CloseOrTerminalStage {

    private final @Nullable ResponseHeaderData header;
    private final @Nullable ApiMessage body;
    private boolean closeConnection;

    RouterResultBuilderImpl(
                            @Nullable ResponseHeaderData header,
                            @Nullable ApiMessage body) {
        this.header = header;
        this.body = body;
    }

    @Override
    public TerminalStage withCloseConnection() {
        closeConnection = true;
        return this;
    }

    @Override
    public RouterResponse build() {
        return new RouterResultImpl(header, body, closeConnection);
    }

    @Override
    public CompletionStage<RouterResponse> completed() {
        return CompletableFuture.completedStage(build());
    }
}
