/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.codec;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A decoded response frame.
 * @param <B> the decoded ApiMessage type
 */
public class DecodedResponseFrame<B extends ApiMessage>
                                 extends DecodedFrame<ResponseHeaderData, B>
                                 implements Frame {

    /**
     * Create a decoded response frame
     * @param apiVersion apiVersion
     * @param correlationId correlationId
     * @param header header
     * @param body body
     */
    public DecodedResponseFrame(short apiVersion, int correlationId, ResponseHeaderData header, B body) {
        super(apiVersion, correlationId, header, body);
    }

    public short headerVersion() {
        return apiKey().messageType.responseHeaderVersion(apiVersion);
    }
}
