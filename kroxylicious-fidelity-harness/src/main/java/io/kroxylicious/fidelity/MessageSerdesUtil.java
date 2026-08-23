/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.fidelity;

import java.nio.ByteBuffer;

public class MessageSerdesUtil {

    /* This utility class should not be instantiated */
    private MessageSerdesUtil() {
    }

    public static byte[] asByteArray(ByteBuffer buffer) {
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
