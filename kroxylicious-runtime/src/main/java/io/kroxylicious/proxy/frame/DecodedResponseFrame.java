/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

/**
 * A decoded response frame.
 *
 * @param <B> the type of the response body
 */
public class DecodedResponseFrame<B extends ApiMessage>
        extends DecodedFrame<ResponseHeaderData, B>
        implements ResponseFrame {

    /**
     * Creates a decoded response frame.
     *
     * @param apiVersion the API version of the response
     * @param correlationId the correlation id of the response
     * @param header the response header
     * @param body the response body
     */
    public DecodedResponseFrame(short apiVersion, int correlationId, ResponseHeaderData header, B body) {
        super(apiVersion, correlationId, header, body);
    }

    @Override
    public short headerVersion() {
        return ApiKeys.forId(apiKeyId()).messageType.responseHeaderVersion(apiVersion);
    }
}
