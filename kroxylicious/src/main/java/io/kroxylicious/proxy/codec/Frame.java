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
package io.kroxylicious.proxy.codec;

import io.netty.buffer.ByteBuf;

/**
 * A frame in the Kafka protocol, which may or may not be fully decoded.
 */
public interface Frame {

    /**
     * Estimate the expected encoded size in bytes of this {@code Frame}.<br>
     * In particular, written data by {@link #encode(ByteBuf)} should be the same as reported by this method.
     */
    int estimateEncodedSize();

    /**
     * Write the frame, including the size prefix, to the given buffer
     * @param out The output buffer
     */
    void encode(ByteBuf out);

    /**
     * The correlation id.
     * @return The correlation id.
     */
    int correlationId();

}
