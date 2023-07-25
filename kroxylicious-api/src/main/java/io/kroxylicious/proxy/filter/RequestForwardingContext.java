/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

public interface RequestForwardingContext extends BaseKrpcFilterContext {
    /**
     * Send a request towards the broker, invoking upstream filters.
     *
     * @param header The header to forward to the broker.
     * @param request The request to forward to the broker.
     */
    void forwardRequest(RequestHeaderData header, ApiMessage request);
}
