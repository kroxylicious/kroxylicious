/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

/**
 * Combined stage of the {@link RouterContext} builder API that allows
 * both closing the connection and building the result.
 */
public interface CloseOrTerminalStage extends TerminalStage, CloseStage {
}
