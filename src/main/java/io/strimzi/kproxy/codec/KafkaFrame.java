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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

public class KafkaFrame {
    private final ApiMessage header;
    private final ApiMessage body;
    private final short apiVersion;

    public KafkaFrame(short apiVersion, ApiMessage header, ApiMessage body) {
        this.header = header;
        this.apiVersion = apiVersion;
        this.body = body;
    }

    public ApiMessage header() {
        return header;
    }

    public ApiMessage body() {
        return body;
    }

    public short headerVersion() {
        return apiKey().messageType.requestHeaderVersion(apiVersion);
    }

    private ApiKeys apiKey() {
        return ApiKeys.forId(body.apiKey());
    }

    public short apiVersion() {
        return apiVersion;
    }

    @Override
    public String toString() {
        return "KafkaFrame(" +
                "apiVersion=" + apiVersion +
                ", header=" + header +
                ", body=" + body +
                ')';
    }
}
