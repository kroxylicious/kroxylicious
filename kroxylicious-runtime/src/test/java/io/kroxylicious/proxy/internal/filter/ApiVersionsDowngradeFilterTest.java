/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.ApiVersionsServiceImpl;
import io.kroxylicious.proxy.internal.FilterHarness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiVersionsDowngradeFilterTest extends FilterHarness {

    public static final ObjectSerializationCache CACHE = new ObjectSerializationCache();

    @Test
    void shortCircuitDowngradeApiVersionsRequests() {
        buildChannel(new ApiVersionsDowngradeFilter(new ApiVersionsServiceImpl()));
        writeRequest(ApiVersionsDowngradeFilter.downgradeApiVersionsFrame(5));
        DecodedResponseFrame<ApiVersionsResponseData> response = channel.readInbound();
        assertThat(response.body()).isInstanceOfSatisfying(ApiVersionsResponseData.class, apiVersionsResponseData -> {
            assertThat(apiVersionsResponseData.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
            ApiVersionsResponseData.ApiVersion apiVersion = new ApiVersionsResponseData.ApiVersion();
            apiVersion.setApiKey(ApiKeys.API_VERSIONS.id);
            apiVersion.setMinVersion(ApiKeys.API_VERSIONS.oldestVersion());
            apiVersion.setMaxVersion(ApiKeys.API_VERSIONS.latestVersion(true));
            assertThat(apiVersionsResponseData.apiKeys()).containsExactly(apiVersion);
        });
    }

    @Test
    void shortCircuitDowngradeApiVersionsRequestsConsidersLatestVersionOverride() {
        buildChannel(new ApiVersionsDowngradeFilter(new ApiVersionsServiceImpl(Map.of(ApiKeys.API_VERSIONS, (short) 2))));
        writeRequest(ApiVersionsDowngradeFilter.downgradeApiVersionsFrame(5));
        DecodedResponseFrame<ApiVersionsResponseData> response = channel.readInbound();
        assertThat(response.body()).isInstanceOfSatisfying(ApiVersionsResponseData.class, apiVersionsResponseData -> {
            assertThat(apiVersionsResponseData.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
            ApiVersionsResponseData.ApiVersion apiVersion = new ApiVersionsResponseData.ApiVersion();
            apiVersion.setApiKey(ApiKeys.API_VERSIONS.id);
            apiVersion.setMinVersion(ApiKeys.API_VERSIONS.oldestVersion());
            apiVersion.setMaxVersion((short) 2);
            assertThat(apiVersionsResponseData.apiKeys()).containsExactly(apiVersion);
        });
    }

    @Test
    void passThroughAnythingElse() {
        buildChannel(new ApiVersionsDowngradeFilter(new ApiVersionsServiceImpl()));
        DecodedRequestFrame<ApiVersionsRequestData> request = writeRequest(new ApiVersionsRequestData());
        var propagated = channel.readOutbound();
        assertThat(propagated).isEqualTo(request);
    }

    @Test
    void downgradeFrameHeaderNotWritable() {
        DecodedRequestFrame<ApiVersionsRequestData> frame = ApiVersionsDowngradeFilter.downgradeApiVersionsFrame(5);
        RequestHeaderData header = frame.header();
        assertThatThrownBy(() -> header.size(CACHE, (short) 1)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> header.write(new ByteBufferAccessor(ByteBuffer.allocate(1)), CACHE, (short) 1)).isInstanceOf(
                UnsupportedOperationException.class);
    }

    @Test
    void downgradeFrameBodyNotWritable() {
        DecodedRequestFrame<ApiVersionsRequestData> frame = ApiVersionsDowngradeFilter.downgradeApiVersionsFrame(5);
        ApiVersionsRequestData body = frame.body();
        assertThatThrownBy(() -> body.size(CACHE, (short) 1)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> body.write(new ByteBufferAccessor(ByteBuffer.allocate(1)), CACHE, (short) 1)).isInstanceOf(
                UnsupportedOperationException.class);
    }
}
