/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.kproxy.codec;

import java.nio.ByteBuffer;
import java.util.Random;

import io.netty.buffer.Unpooled;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ByteBufAccessorTest {
    @Test
    public void testRead() {
        var bbuffer = ByteBuffer.allocate(1024);

        Random rng = new Random();
        for (int i = 0; i < 1000; i++) {
            // Write using Kafka accessor
            ByteBufferAccessor nio = new ByteBufferAccessor(bbuffer);
            byte writtenByte = (byte) rng.nextInt();
            nio.writeByte(writtenByte);
            byte[] writtenArray = {1, 2, 3};
            nio.writeByteArray(writtenArray);
            double writtenDouble = rng.nextDouble();
            nio.writeDouble(writtenDouble);
            int writtenInt = rng.nextInt();
            nio.writeInt(writtenInt);
            long writtenLong = rng.nextLong();
            nio.writeLong(writtenLong);
            short writtenShort = (short) rng.nextInt();
            nio.writeShort(writtenShort);
            int writtenUnsignedVarint = rng.nextInt();
            nio.writeUnsignedVarint(writtenUnsignedVarint);
            int writtenVarint = rng.nextInt();
            nio.writeVarint(writtenVarint);
            long writtenVarlong = rng.nextLong();
            nio.writeVarlong(writtenVarlong);

            // Read using ByteBuf accessor
            var bbuf = Unpooled.wrappedBuffer(bbuffer.flip());
            var kp = new ByteBufAccessor(bbuf);
            assertEquals(writtenByte, kp.readByte());
            var readArray = new byte[writtenArray.length];
            kp.readArray(readArray);
            assertArrayEquals(writtenArray, readArray);
            assertEquals(writtenDouble, kp.readDouble());
            assertEquals(writtenInt, kp.readInt());
            assertEquals(writtenLong, kp.readLong());
            assertEquals(writtenShort, kp.readShort());
            assertEquals(writtenUnsignedVarint, kp.readUnsignedVarint());
            assertEquals(writtenVarint, kp.readVarint());
            assertEquals(writtenVarlong, kp.readVarlong());

            bbuffer.clear();
        }
    }

    @Test
    public void testWrite() {
        var bbuf = Unpooled.buffer(1024);

        Random rng = new Random();
        for (int i = 0; i < 1000; i++) {
            // Write using ByteBuffer classes
            var accessor = new ByteBufAccessor(bbuf);
            byte writtenByte = (byte) rng.nextInt();
            accessor.writeByte(writtenByte);
            byte[] writtenArray = {1, 2, 3};
            accessor.writeByteArray(writtenArray);
            double writtenDouble = rng.nextDouble();
            accessor.writeDouble(writtenDouble);
            int writtenInt = rng.nextInt();
            accessor.writeInt(writtenInt);
            long writtenLong = rng.nextLong();
            accessor.writeLong(writtenLong);
            short writtenShort = (short) rng.nextInt();
            accessor.writeShort(writtenShort);
            int writtenUnsignedVarint = rng.nextInt();
            accessor.writeUnsignedVarint(writtenUnsignedVarint);
            int writtenVarint = rng.nextInt();
            accessor.writeVarint(writtenVarint);
            long writtenVarlong = rng.nextLong();
            accessor.writeVarlong(writtenVarlong);

            // Read using Kafka accessor
            var nio = ByteBuffer.wrap(bbuf.array());
            var kafkaAccessor = new ByteBufferAccessor(nio);
            assertEquals(writtenByte, kafkaAccessor.readByte());
            var readArray = new byte[writtenArray.length];
            kafkaAccessor.readArray(readArray);
            assertArrayEquals(writtenArray, readArray);
            assertEquals(writtenDouble, kafkaAccessor.readDouble());
            assertEquals(writtenInt, kafkaAccessor.readInt());
            assertEquals(writtenLong, kafkaAccessor.readLong());
            assertEquals(writtenShort, kafkaAccessor.readShort());
            assertEquals(writtenUnsignedVarint, kafkaAccessor.readUnsignedVarint());
            assertEquals(writtenVarint, kafkaAccessor.readVarint());
            assertEquals(writtenVarlong, kafkaAccessor.readVarlong());

            bbuf.clear();
        }
    }
}