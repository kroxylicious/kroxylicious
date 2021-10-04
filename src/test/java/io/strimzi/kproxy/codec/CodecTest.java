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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

public class CodecTest {
    public static Stream<Short> requestApiVersions() {
        return requestApiVersions(ApiMessageType.API_VERSIONS);
    }

    public static Stream<Short> requestApiVersions(ApiMessageType type) {
        return IntStream.rangeClosed(type.lowestSupportedVersion(), type.highestSupportedVersion())
                .mapToObj(version -> (short) version);
    }

    protected static ApiVersionsRequestData exampleApiVersionsRequest() {
        return new ApiVersionsRequestData()
                .setClientSoftwareName("foo/bar")
                .setClientSoftwareVersion("1.2.0");
    }

    protected static ApiVersionsResponseData exampleApiVersionsResponse() {
        ApiVersionsResponseData.ApiVersionCollection ak = new ApiVersionsResponseData.ApiVersionCollection();
        for (var apiKey : ApiKeys.values()) {
            ak.add(new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(apiKey.id)
                    .setMinVersion(apiKey.messageType.lowestSupportedVersion())
                    .setMaxVersion(apiKey.messageType.highestSupportedVersion()));
        }

        return new ApiVersionsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(12)
                .setFinalizedFeaturesEpoch(1)
                .setApiKeys(ak)
                .setSupportedFeatures(new ApiVersionsResponseData.SupportedFeatureKeyCollection())
                .setFinalizedFeatures(new ApiVersionsResponseData.FinalizedFeatureKeyCollection());
    }

    protected static RequestHeaderData exampleRequestHeader(short apiVersion) {
        return new RequestHeaderData()
                .setClientId("fooooo")
                .setCorrelationId(12)
                .setRequestApiKey(ApiKeys.API_VERSIONS.id)
                .setRequestApiVersion(apiVersion);
    }

    protected static ResponseHeaderData exampleResponseHeader() {
        return new ResponseHeaderData()
                .setCorrelationId(12);
    }

    protected static RequestHeaderData deserializeRequestHeaderUsingKafkaApis(short headerVersion, ByteBuffer buffer) {
        return new RequestHeaderData(new ByteBufferAccessor(buffer), headerVersion);
    }

    protected static ResponseHeaderData deserializeResponseHeaderUsingKafkaApis(short headerVersion, ByteBuffer buffer) {
        return new ResponseHeaderData(new ByteBufferAccessor(buffer), headerVersion);
    }

    protected static ApiVersionsRequestData deserializeApiVersionsRequestUsingKafkaApis(short apiVersion, ByteBuffer buffer) {
        return new ApiVersionsRequestData(new ByteBufferAccessor(buffer), apiVersion);
    }

    protected static ApiVersionsResponseData deserializeApiVersionsResponseUsingKafkaApis(short apiVersion, ByteBuffer buffer) {
        return new ApiVersionsResponseData(new ByteBufferAccessor(buffer), apiVersion);
    }

    protected static ByteBuffer serializeUsingKafkaApis(short headerVersion, ApiMessage header, short apiVersion, ApiMessage body) {
        return serializeUsingKafkaApis(1, headerVersion, header, apiVersion, body);
    }

    protected static ByteBuffer serializeUsingKafkaApis(int numCopies, short headerVersion, ApiMessage header, short apiVersion, ApiMessage body) {
        var cache = new ObjectSerializationCache();
        int headerSize = header.size(cache, headerVersion);
        int bodySize = body.size(cache, apiVersion);
        ByteBuffer bbuffer = ByteBuffer.allocate(numCopies * (4 + headerSize + bodySize));
        for (int i = 0; i < numCopies; i++) {
            bbuffer.putInt(headerSize + bodySize);
            var kafkaAccessor = new ByteBufferAccessor(bbuffer);
            header.write(kafkaAccessor, cache, headerVersion);
            body.write(kafkaAccessor, cache, apiVersion);
        }
        bbuffer.flip();
        return bbuffer;
    }
}
