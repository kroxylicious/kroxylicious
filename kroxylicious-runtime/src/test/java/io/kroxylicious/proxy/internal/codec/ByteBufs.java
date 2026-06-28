/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.codec;

import org.junit.jupiter.api.function.ThrowingConsumer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

public class ByteBufs {

    public static ByteBuf writeByteBuf(ThrowingConsumer<ByteBufOutputStream> consumer) {
        try (ByteBufOutputStream stream = new ByteBufOutputStream(Unpooled.buffer())) {
            consumer.accept(stream);
            return stream.buffer();
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
