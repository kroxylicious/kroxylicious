/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

public interface ResponseForwardingContext extends BaseKrpcFilterContext {
    /**
     * Send a response towards the client, invoking downstream filters.
     * <p>If this is invoked while the message is flowing downstream towards the broker, then
     * it will not be sent to the broker. So this method can be used to generate responses in
     * the proxy.</p>
     *
     * @param header   The header to forward to the client.
     * @param response The response to forward to the client.
     * @throws AssertionError if response is logically inconsistent, for example responding with request data
     *                        or responding with a produce response to a fetch request. It is up to specific implementations to
     *                        determine what logically inconsistent means.
     */
    void forwardResponse(ResponseHeaderData header, ApiMessage response);

    /**
     * Send a response towards the client, invoking downstream filters.
     * <p>If this is invoked while the message is flowing downstream towards the broker, then
     * it will not be sent to the broker. So this method can be used to generate responses in
     * the proxy. In this case response headers will be created with a correlationId matching the request</p>
     * @param response The response to forward to the client.
     * @throws AssertionError if response is logically inconsistent, for example responding with request data
     * or responding with a produce response to a fetch request. It is up to specific implementations to
     * determine what logically inconsistent means.
     */
    void forwardResponse(ApiMessage response);
}
