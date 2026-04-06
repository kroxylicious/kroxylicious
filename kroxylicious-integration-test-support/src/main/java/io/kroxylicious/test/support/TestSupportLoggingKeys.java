/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.support;

/**
 * Common keys for structured logging in kroxylicious-integration-test-support.
 */
public class TestSupportLoggingKeys {

    private TestSupportLoggingKeys() {
    }

    /**
     * Number of bytes already read from a buffer or stream.
     */
    public static final String ALREADY_READ = "alreadyRead";

    /**
     * Kafka API identifier.
     */
    public static final String API_ID = "apiId";

    /**
     * The Kafka API key identifying the request or response type.
     */
    public static final String API_KEY = "apiKey";

    /**
     * The Kafka protocol API version.
     */
    public static final String API_VERSION = "apiVersion";

    /**
     * HTTP or protocol message body.
     */
    public static final String BODY = "body";

    /**
     * Correlation identifier.
     */
    public static final String CORRELATION = "correlation";

    /**
     * Correlation ID for request/response matching.
     */
    public static final String CORRELATION_ID = "correlationId";

    /**
     * Netty channel handler context.
     */
    public static final String CTX = "ctx";

    /**
     * Whether request decoding is enabled.
     */
    public static final String DECODE_REQUEST = "decodeRequest";

    /**
     * Whether response decoding is enabled.
     */
    public static final String DECODE_RESPONSE = "decodeResponse";

    /**
     * Size of encoded message in bytes.
     */
    public static final String ENCODED_SIZE = "encodedSize";

    /**
     * Kafka protocol frame.
     */
    public static final String FRAME = "frame";

    /**
     * Fully-qualified frame class name.
     */
    public static final String FRAME_CLASS = "frameClass";

    /**
     * Size of protocol frame in bytes.
     */
    public static final String FRAME_SIZE = "frameSize";

    /**
     * Protocol message header.
     */
    public static final String HEADER = "header";

    /**
     * Protocol header version number.
     */
    public static final String HEADER_VERSION = "headerVersion";

    /**
     * Output channel or destination.
     */
    public static final String OUT = "out";

    /**
     * Number of readable bytes in a buffer.
     */
    public static final String READABLE_BYTES = "readableBytes";

    /**
     * Correlation ID for upstream broker requests.
     */
    public static final String UPSTREAM_CORRELATION_ID = "upstreamCorrelationId";
}
