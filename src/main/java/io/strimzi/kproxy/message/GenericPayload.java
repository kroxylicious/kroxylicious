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
package io.strimzi.kproxy.message;

import io.netty.buffer.ByteBuf;

public class GenericPayload {
    private final short apiKey;
    private final short apiVersion;
    private final ByteBuf payload;

    public GenericPayload(short apiKey, short apiVersion, ByteBuf payload) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.payload = payload;
    }

    public short apiKey() {
        return apiKey;
    }

    public short apiVersion() {
        return apiVersion;
    }

    public ByteBuf payload() {
        return payload;
    }
}
