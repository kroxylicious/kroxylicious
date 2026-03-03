/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiVersionMaxVersionLimiterTest extends ApiVersionsResponseTransformerTester {

    static Stream<Arguments> testApiVersionMaxVersionLimiter() {
        return Stream.of(
                Arguments.argumentSet("actual max higher than limit", (short) 12, (short) 5, (short) 13, (short) 5, (short) 12),
                Arguments.argumentSet("actual max the same as limit", (short) 12, (short) 5, (short) 12, (short) 5, (short) 12),
                Arguments.argumentSet("actual max below limit", (short) 12, (short) 5, (short) 11, (short) 5, (short) 11),
                Arguments.argumentSet("minimum at the limit", (short) 12, (short) 12, (short) 12, (short) 12, (short) 12));
    }

    @MethodSource
    @ParameterizedTest
    void testApiVersionMaxVersionLimiter(short maxVersionLimit, short inputMinVersion, short inputMaxVersion, short expectedMinVersion, short expectedMaxVersion) {
        ApiVersionsResponseTransformer limiter = ApiVersionsResponseTransformers.limitMaxVersionForApiKeys(Map.of(ApiKeys.PRODUCE, maxVersionLimit));
        ApiVersionsResponseData input = apiVersionsResponseData(apiVersion(ApiKeys.PRODUCE, inputMinVersion, inputMaxVersion));
        ApiVersionsResponseData apiVersionsResponseData = limiter.transform(input);
        assertVersionsForApiKey(apiVersionsResponseData, ApiKeys.PRODUCE, expectedMinVersion, expectedMaxVersion);
    }

    @Test
    void limitMultipleApiVersions() {
        ApiVersionsResponseTransformer limiter = ApiVersionsResponseTransformers.limitMaxVersionForApiKeys(Map.of(ApiKeys.PRODUCE, (short) 5, ApiKeys.FETCH, (short) 6));
        ApiVersionsResponseData input = apiVersionsResponseData(apiVersion(ApiKeys.PRODUCE, (short) 0, (short) 6), apiVersion(ApiKeys.FETCH, (short) 0, (short) 7));
        ApiVersionsResponseData apiVersionsResponseData = limiter.transform(input);
        assertVersionsForApiKey(apiVersionsResponseData, ApiKeys.PRODUCE, (short) 0, (short) 5);
        assertVersionsForApiKey(apiVersionsResponseData, ApiKeys.FETCH, (short) 0, (short) 6);
    }

    @Test
    void otherApiVersionsUntouched() {
        ApiVersionsResponseTransformer limiter = ApiVersionsResponseTransformers.limitMaxVersionForApiKeys(Map.of(ApiKeys.PRODUCE, (short) 5));
        ApiVersionsResponseData input = apiVersionsResponseData(apiVersion(ApiKeys.PRODUCE, (short) 0, (short) 6), apiVersion(ApiKeys.FETCH, (short) 0, (short) 7));
        ApiVersionsResponseData apiVersionsResponseData = limiter.transform(input);
        assertVersionsForApiKey(apiVersionsResponseData, ApiKeys.PRODUCE, (short) 0, (short) 5);
        assertVersionsForApiKey(apiVersionsResponseData, ApiKeys.FETCH, (short) 0, (short) 7);
    }

    @Test
    void throwsIfMinimumVersionIsAboveTheLimit() {
        ApiVersionsResponseTransformer limiter = ApiVersionsResponseTransformers.limitMaxVersionForApiKeys(Map.of(ApiKeys.PRODUCE, (short) 5));
        ApiVersionsResponseData input = apiVersionsResponseData(apiVersion(ApiKeys.PRODUCE, (short) 6, (short) 6));
        assertThatThrownBy(() -> {
            limiter.transform(input);
        }).isInstanceOf(ApiVersionsTransformationException.class)
                .hasMessage("upstream advertised min version 6 for PRODUCE is above the limit of 5 imposed by this transformer");
    }

    @Test
    void andChains() {
        ApiVersionsResponseTransformer limiter = ApiVersionsResponseTransformers.limitMaxVersionForApiKeys(Map.of(ApiKeys.PRODUCE, (short) 5));
        ApiVersionsResponseTransformer limiter2 = ApiVersionsResponseTransformers.limitMaxVersionForApiKeys(Map.of(ApiKeys.FETCH, (short) 6));
        ApiVersionsResponseTransformer andTranformer = limiter.and(limiter2);
        ApiVersionsResponseData input = apiVersionsResponseData(apiVersion(ApiKeys.PRODUCE, (short) 1, (short) 6), apiVersion(ApiKeys.FETCH, (short) 1, (short) 7));
        ApiVersionsResponseData apiVersionsResponseData = andTranformer.transform(input);
        assertVersionsForApiKey(apiVersionsResponseData, ApiKeys.PRODUCE, (short) 1, (short) 5);
        assertVersionsForApiKey(apiVersionsResponseData, ApiKeys.FETCH, (short) 1, (short) 6);
    }

}