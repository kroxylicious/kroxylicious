/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.codec;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A decoded request frame.
 * @param <B> type of api message in decoded frame
 */
public class DecodedRequestFrame<B extends ApiMessage>
        extends DecodedFrame<RequestHeaderData, B>
        implements Frame {

    /**
     * Create a decoded request frame
     * @param apiVersion apiVersion
     * @param correlationId correlationId
     * @param header header
     * @param body body
     */
    public DecodedRequestFrame(short apiVersion,
                               int correlationId,
                               RequestHeaderData header,
                               B body) {
        super(apiVersion, correlationId, header, body);
    }

    @Override
    public short headerVersion() {
        return apiKey().messageType.requestHeaderVersion(apiVersion);
    }

}
