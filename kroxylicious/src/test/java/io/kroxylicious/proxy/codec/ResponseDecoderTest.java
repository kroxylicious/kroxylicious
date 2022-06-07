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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ResponseDecoderTest extends AbstractCodecTest {

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsExactlyOneFrame_decoded(short apiVersion) throws Exception {
        exactlyOneFrame_decoded(apiVersion,
                ApiKeys.API_VERSIONS::responseHeaderVersion,
                v -> AbstractCodecTest.exampleResponseHeader(),
                AbstractCodecTest::exampleApiVersionsResponse,
                AbstractCodecTest::deserializeResponseHeaderUsingKafkaApis,
                AbstractCodecTest::deserializeApiVersionsResponseUsingKafkaApis,
                new KafkaResponseDecoder(new HashMap<>(Map.of(12, new Correlation(ApiKeys.API_VERSIONS, apiVersion, true)))),
                DecodedResponseFrame.class);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testApiVersionsExactlyOneFrame_opqaue(short apiVersion) throws Exception {
        exactlyOneFrame_encoded(apiVersion,
                ApiKeys.API_VERSIONS::responseHeaderVersion,
                v -> AbstractCodecTest.exampleResponseHeader(),
                AbstractCodecTest::exampleApiVersionsResponse,
                new KafkaResponseDecoder(new HashMap<>(Map.of(12, new Correlation(ApiKeys.API_VERSIONS, apiVersion, false)))),
                OpaqueResponseFrame.class);
    }

}
