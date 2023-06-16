/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A decoded response frame.
 */
public class DecodedResponseFrame
        extends DecodedFrame<ResponseHeaderData>
        implements ResponseFrame {

    public DecodedResponseFrame(short apiVersion, int correlationId, ResponseHeaderData header, ApiMessage body) {
        super(apiVersion, correlationId, header, ResponseHeaderData.class, body);
    }

    public short headerVersion() {
        return apiKey().messageType.responseHeaderVersion(apiVersion);
    }
}
