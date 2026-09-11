/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

/**
 * Stage of the {@link RouterContext} builder API that allows
 * requesting that the client connection be closed.
 */
public interface CloseStage {
    /**
     * Signals that the client connection should be closed after
     * the result is delivered.
     *
     * @return the terminal stage
     */
    TerminalStage withCloseConnection();
}
