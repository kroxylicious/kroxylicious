/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A decoded response frame.
 */
public class DecodedResponseFrame<B extends ApiMessage>
        extends DecodedFrame<ResponseHeaderData, B>
        implements ResponseFrame {

    public DecodedResponseFrame(short apiVersion, int correlationId, ResponseHeaderData header, B body) {
        super(apiVersion, correlationId, header, body);
    }

    public short headerVersion() {
        return ApiKeys.forId(apiKeyId()).messageType.responseHeaderVersion(apiVersion);
    }
}
