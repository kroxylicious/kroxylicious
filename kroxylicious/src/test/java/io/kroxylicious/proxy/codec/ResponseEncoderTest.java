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

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ResponseEncoderTest extends AbstractCodecTest {
    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersions(short apiVersion) throws Exception {
        ResponseHeaderData exampleHeader = exampleResponseHeader();
        ApiVersionsResponseData exampleBody = exampleApiVersionsResponse();
        short headerVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(apiVersion);
        ByteBuffer expected = serializeUsingKafkaApis(headerVersion, exampleHeader, apiVersion, exampleBody);
        testEncode(expected, new DecodedResponseFrame<>(apiVersion, exampleHeader.correlationId(), exampleHeader, exampleBody), new KafkaResponseEncoder());
    }
}
