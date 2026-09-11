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
package io.kroxylicious.kafka.common.compress;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import io.kroxylicious.kafka.common.record.internal.CompressionType;
import io.kroxylicious.kafka.common.utils.BufferSupplier;
import io.kroxylicious.kafka.common.utils.ByteBufferInputStream;
import io.kroxylicious.kafka.common.utils.ByteBufferOutputStream;

public class NoCompression implements Compression {

    private NoCompression() {
    }

    @Override
    public CompressionType type() {
        return CompressionType.NONE;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        return bufferStream;
    }

    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        return new ByteBufferInputStream(buffer);
    }

    public static class Builder implements Compression.Builder<NoCompression> {

        @Override
        public NoCompression build() {
            return new NoCompression();
        }
    }
}
