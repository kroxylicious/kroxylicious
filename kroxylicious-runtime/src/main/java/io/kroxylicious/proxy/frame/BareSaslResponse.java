/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import static io.kroxylicious.proxy.frame.LazyRequestResponseState.lazyRequestResponseState;

/**
 * Ancient versions of Kafka implemented SASL/GSSAPI by sending the response
 * on the wire as length prefixed bytes (no Kafka protocol header).
 * This frame represents those kinds of response.
 */
public class BareSaslResponse implements ResponseFrame {

    private final byte[] bytes;
    private final RequestResponseState requestResponseState = lazyRequestResponseState();

    public BareSaslResponse(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public int estimateEncodedSize() {
        return bytes.length;
    }

    @Override
    public void encode(ByteBufAccessor out) {
        out.writeByteArray(bytes);
    }

    @Override
    public int correlationId() {
        return 0;
    }

    @Override
    public RequestResponseState requestResponseState() {
        return requestResponseState;
    }

    public byte[] bytes() {
        return bytes;
    }
}
