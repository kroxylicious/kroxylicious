/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import org.apache.kafka.common.protocol.ApiMessage;

/**
 * The outcome of {@link Router#onRequest} processing.
 *
 * <p>Instances are created via the builder methods on {@link RouterContext}:
 * {@link RouterContext#respondWith(ApiMessage) respondWith},
 * {@link RouterContext#respondWithError respondWithError}, and
 * {@link RouterContext#respondWithoutReply() respondWithoutReply}.</p>
 */
public interface RouterResponse {
}
