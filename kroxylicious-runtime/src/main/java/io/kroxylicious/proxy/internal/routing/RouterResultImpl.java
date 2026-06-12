/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.router.RouterResponse;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Concrete implementation of {@link RouterResponse}.
 */
class RouterResultImpl implements RouterResponse {

    private final @Nullable ResponseHeaderData header;
    private final @Nullable ApiMessage body;
    private final boolean closeConnection;

    RouterResultImpl(
                     @Nullable ResponseHeaderData header,
                     @Nullable ApiMessage body,
                     boolean closeConnection) {
        this.header = header;
        this.body = body;
        this.closeConnection = closeConnection;
    }

    @Nullable
    ResponseHeaderData header() {
        return header;
    }

    @Nullable
    ApiMessage body() {
        return body;
    }

    boolean hasBody() {
        return body != null;
    }

    boolean closeConnection() {
        return closeConnection;
    }
}
