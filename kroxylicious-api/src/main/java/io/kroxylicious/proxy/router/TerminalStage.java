/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import java.util.concurrent.CompletionStage;

/**
 * Terminal stage of the {@link RouterContext} builder API.
 */
public interface TerminalStage {
    /**
     * Constructs the router result.
     *
     * @return router result
     */
    RouterResponse build();

    /**
     * Produces the router result contained within a completed {@link CompletionStage}.
     *
     * @return completion stage containing the router result
     */
    CompletionStage<RouterResponse> completed();
}
