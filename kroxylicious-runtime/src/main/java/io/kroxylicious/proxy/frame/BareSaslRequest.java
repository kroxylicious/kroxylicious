/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

/**
 * Ancient versions of Kafka implemented SASL/GSSAPI by sending the token
 * on the wire as length prefixed bytes (no Kafka protocol header).
 * This frame represents those kinds of request.
 *
 * @see "org.apache.kafka.common.security.authenticator.SaslServerAuthenticator#handleKafkaRequest()"
 */
public class BareSaslRequest implements RequestFrame {

    private final byte[] bytes;
    private final boolean decodeResponse;

    public BareSaslRequest(byte[] bytes, boolean decodeResponse) {
        this.bytes = bytes;
        this.decodeResponse = decodeResponse;
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
    public short apiKeyId() {
        return -1;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public boolean isDecoded() {
        return true;
    }

    @Override
    public boolean decodeResponse() {
        return decodeResponse;
    }

    public byte[] bytes() {
        return bytes;
    }
}
