/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.admin.reload.handlers;

import io.kroxylicious.proxy.internal.admin.reload.ReloadException;
import io.kroxylicious.proxy.internal.admin.reload.ReloadRequestContext;

/**
 * Handler interface for the Chain of Responsibility pattern in request processing.
 * Each handler processes one concern (validation, parsing, execution) and passes
 * the context to the next handler in the chain.
 */
public interface ReloadRequestHandler {

    /**
     * Process the request context and return an updated context.
     *
     * @param context Current request context
     * @return Updated context for next handler
     * @throws ReloadException If processing fails
     */
    ReloadRequestContext handle(ReloadRequestContext context) throws ReloadException;
}
