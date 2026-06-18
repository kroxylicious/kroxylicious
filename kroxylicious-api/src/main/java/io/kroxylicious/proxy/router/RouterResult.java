/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

/**
 * The outcome of {@link Router#onRequest} processing. Encodes the result
 * as a value rather than a side-effect on the context.
 */
public sealed interface RouterResult {

    /**
     * The router has produced a response for the client. The runtime delivers
     * the response, automatically rewriting the correlation ID to match the
     * client's original request.
     *
     * @param response the response to deliver to the client
     */
    record Completed(Response response) implements RouterResult {}

    /**
     * The router has finished processing but there is no response to deliver.
     * This is the correct result for fire-and-forget requests (e.g.
     * {@code PRODUCE} with {@code acks=0}).
     */
    record CompletedNoResponse() implements RouterResult {}

    /**
     * The router requests that the client connection be closed.
     */
    record Disconnect() implements RouterResult {}
}
