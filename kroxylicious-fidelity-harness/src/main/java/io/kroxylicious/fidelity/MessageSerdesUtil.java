/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.fidelity;

import java.nio.ByteBuffer;

/**
 * Utility class to extract common code between Kafka and Kroxylicious Serdes
 */
public class MessageSerdesUtil {

    /* This utility class should not be instantiated */
    private MessageSerdesUtil() {
    }

    /**
     * Extracts a byte array of all the populated data from the byte bugger
     * @param buffer to extract the raw bytes from.
     * @return A byte[] containing the populated bytes of the buffer.
     */
    public static byte[] asByteArray(ByteBuffer buffer) {
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
