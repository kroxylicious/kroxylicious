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

import io.netty.buffer.Unpooled;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ApiVersionsTest {
    public static Stream<Object[]> requestApiVersions() {
        return IntStream.range(0, ApiVersionsRequestData.SCHEMAS.length)
                .mapToObj(index -> new Object[]{(short) (ApiVersionsRequestData.LOWEST_SUPPORTED_VERSION + index), ApiVersionsRequestData.SCHEMAS[index]});
    }

    public static Stream<Object[]> responseApiVersions() {
        return IntStream.range(0, ApiVersionsResponseData.SCHEMAS.length)
                .mapToObj(index -> new Object[]{(short) (ApiVersionsResponseData.LOWEST_SUPPORTED_VERSION + index), ApiVersionsResponseData.SCHEMAS[index]});
    }

    private void assertSameRequest(Schema schema, ApiVersionsRequestData message, ApiVersionsRequestData readReq) {
        assertEquals(schema.get("client_software_name") != null ? message.clientSoftwareName() : "", readReq.clientSoftwareName());
        assertEquals(schema.get("client_software_version") != null ? message.clientSoftwareVersion() : "", readReq.clientSoftwareVersion());
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testReadApiVersionsRequest(short apiVersion, Schema schema) {
        // Write using Kafka API
        var message = new ApiVersionsRequestData()
                .setClientSoftwareName("foo/bar")
                .setClientSoftwareVersion("1.2.0");
        var cache = new ObjectSerializationCache();
        int messageSize = message.size(cache, apiVersion);
        ByteBuffer bbuffer = ByteBuffer.allocate(messageSize);
        var kafkaAccessor = new ByteBufferAccessor(bbuffer);
        message.write(kafkaAccessor, cache, apiVersion);
        bbuffer.flip();

        // Read using our API
        var bbuf = Unpooled.wrappedBuffer(bbuffer);
        var readReq = new ApiVersionsRequestData(new ByteBufAccessor(bbuf), apiVersion);

        assertSameRequest(schema, message, readReq);
    }

    @ParameterizedTest
    @MethodSource("requestApiVersions")
    public void testWriteApiVersionsRequest(short apiVersion, Schema schema) {
        // Write using our API
        var message = new ApiVersionsRequestData()
                .setClientSoftwareName("foo/bar")
                .setClientSoftwareVersion("1.2.0");
        var cache = new ObjectSerializationCache();
        int messageSize = message.size(cache, apiVersion);
        var bbuf = Unpooled.buffer(messageSize);
        var ourAccessor = new ByteBufAccessor(bbuf);
        message.write(ourAccessor, cache, apiVersion);

        // Read using the Kafka API
        var readReq = new ApiVersionsRequestData(new ByteBufferAccessor(ByteBuffer.wrap(bbuf.array())), apiVersion);
        assertSameRequest(schema, message, readReq);
    }

    private void assertSameResponse(short apiVersion, Schema schema, ApiVersionsResponseData message, ApiVersionsResponseData readReq) {
        // structural fields
        assertEquals(schema.get("error_code") != null ? message.errorCode() : (short) 0, readReq.errorCode());
        assertEquals(schema.get("throttle_time_ms") != null ? message.throttleTimeMs() : 0, readReq.throttleTimeMs());
        assertEquals(message.apiKeys(), readReq.apiKeys());
        // tagged fields
        assertEquals(apiVersion >= 3 ? message.finalizedFeaturesEpoch() : -1L, readReq.finalizedFeaturesEpoch());
        assertEquals(apiVersion >= 3 ? message.finalizedFeatures() : new ApiVersionsResponseData.FinalizedFeatureKeyCollection(), readReq.finalizedFeatures());
        assertEquals(apiVersion >= 3 ? message.supportedFeatures() : new ApiVersionsResponseData.SupportedFeatureKeyCollection(), readReq.supportedFeatures());
    }

    @ParameterizedTest
    @MethodSource("responseApiVersions")
    public void testReadApiVersionsResponse(short apiVersion, Schema schema) {
        // Write using Kafka API
        var ff = new ApiVersionsResponseData.FinalizedFeatureKeyCollection();
        ff.add(new ApiVersionsResponseData.FinalizedFeatureKey()
                .setName("ff")
                        .setMaxVersionLevel((short) 78)
                        .setMinVersionLevel((short) 77));
        var sf = new ApiVersionsResponseData.SupportedFeatureKeyCollection();
        sf.add(new ApiVersionsResponseData.SupportedFeatureKey()
                .setName("ff")
                .setMaxVersion((short) 88)
                .setMinVersion((short) 87));
        var ak = new ApiVersionsResponseData.ApiVersionCollection();
        ak.add(new ApiVersionsResponseData.ApiVersion()
                .setApiKey(ApiKeys.ADD_OFFSETS_TO_TXN.id)
                .setMinVersion((short) 1)
                .setMaxVersion((short) 3));
        var message = new ApiVersionsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(23)
                .setFinalizedFeaturesEpoch(12)
                .setFinalizedFeatures(ff)
                .setSupportedFeatures(sf)
                .setApiKeys(ak);
        var cache = new ObjectSerializationCache();
        int messageSize = message.size(cache, apiVersion);
        ByteBuffer bbuffer = ByteBuffer.allocate(messageSize);
        var kafkaAccessor = new ByteBufferAccessor(bbuffer);
        message.write(kafkaAccessor, cache, apiVersion);
        bbuffer.flip();

        // Read using our API
        var bbuf = Unpooled.wrappedBuffer(bbuffer);
        var readReq = new ApiVersionsResponseData(new ByteBufAccessor(bbuf), apiVersion);

        assertSameResponse(apiVersion, schema, message, readReq);
    }

    @ParameterizedTest
    @MethodSource("responseApiVersions")
    public void testWriteApiVersionsResponse(short apiVersion, Schema schema) {
        // Write using our API
        var ff = new ApiVersionsResponseData.FinalizedFeatureKeyCollection();
        ff.add(new ApiVersionsResponseData.FinalizedFeatureKey()
                .setName("ff")
                .setMaxVersionLevel((short) 78)
                .setMinVersionLevel((short) 77));
        var sf = new ApiVersionsResponseData.SupportedFeatureKeyCollection();
        sf.add(new ApiVersionsResponseData.SupportedFeatureKey()
                .setName("ff")
                .setMaxVersion((short) 88)
                .setMinVersion((short) 87));
        var ak = new ApiVersionsResponseData.ApiVersionCollection();
        ak.add(new ApiVersionsResponseData.ApiVersion()
                .setApiKey(ApiKeys.ADD_OFFSETS_TO_TXN.id)
                .setMinVersion((short) 1)
                .setMaxVersion((short) 3));
        var message = new ApiVersionsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setThrottleTimeMs(23)
                .setFinalizedFeaturesEpoch(12)
                .setFinalizedFeatures(ff)
                .setSupportedFeatures(sf)
                .setApiKeys(ak);
        var cache = new ObjectSerializationCache();
        int messageSize = message.size(cache, apiVersion);
        var bbuf = Unpooled.buffer(messageSize);
        var ourAccessor = new ByteBufAccessor(bbuf);
        message.write(ourAccessor, cache, apiVersion);

        // Read using Kafka API
        var readReq = new ApiVersionsResponseData(new ByteBufferAccessor(ByteBuffer.wrap(bbuf.array())), apiVersion);

        assertSameResponse(apiVersion, schema, message, readReq);
    }
}
