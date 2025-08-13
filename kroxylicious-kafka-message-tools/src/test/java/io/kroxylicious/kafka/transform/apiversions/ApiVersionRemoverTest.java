/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform.apiversions;

import java.util.Set;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

class ApiVersionRemoverTest extends ApiVersionsInterceptorTester {

    @Test
    void removeSingleApiVersion() {
        ApiVersionsResponseTransformer remover = ApiVersionsResponseTransformers.removeApiKeys(Set.of(ApiKeys.PRODUCE));
        ApiVersionsResponseData input = apiVersionsResponseData(apiVersion(ApiKeys.PRODUCE, (short) 0, (short) 1));
        ApiVersionsResponseData apiVersionsResponseData = remover.onApiVersionsResponse(input);
        assertVersionsForApiKeyNotPresent(apiVersionsResponseData, ApiKeys.PRODUCE);
    }

    @Test
    void removeMultipleApiVersions() {
        ApiVersionsResponseTransformer remover = ApiVersionsResponseTransformers.removeApiKeys(Set.of(ApiKeys.PRODUCE, ApiKeys.FETCH));
        ApiVersionsResponseData input = apiVersionsResponseData(apiVersion(ApiKeys.PRODUCE, (short) 0, (short) 1), apiVersion(ApiKeys.FETCH, (short) 0, (short) 1));
        ApiVersionsResponseData apiVersionsResponseData = remover.onApiVersionsResponse(input);
        assertVersionsForApiKeyNotPresent(apiVersionsResponseData, ApiKeys.PRODUCE);
        assertVersionsForApiKeyNotPresent(apiVersionsResponseData, ApiKeys.FETCH);
    }

    @Test
    void preserveOtherApiKeys() {
        ApiVersionsResponseTransformer remover = ApiVersionsResponseTransformers.removeApiKeys(Set.of(ApiKeys.PRODUCE));
        ApiVersionsResponseData input = apiVersionsResponseData(apiVersion(ApiKeys.PRODUCE, (short) 0, (short) 1), apiVersion(ApiKeys.FETCH, (short) 0, (short) 1));
        ApiVersionsResponseData apiVersionsResponseData = remover.onApiVersionsResponse(input);
        assertVersionsForApiKeyNotPresent(apiVersionsResponseData, ApiKeys.PRODUCE);
        assertVersionsForApiKey(apiVersionsResponseData, ApiKeys.FETCH, (short) 0, (short) 1);
    }

    @Test
    void andChains() {
        ApiVersionsResponseTransformer remover = ApiVersionsResponseTransformers.removeApiKeys(Set.of(ApiKeys.PRODUCE));
        ApiVersionsResponseTransformer remover2 = ApiVersionsResponseTransformers.removeApiKeys(Set.of(ApiKeys.FETCH));
        ApiVersionsResponseTransformer interceptor = remover.and(remover2);
        ApiVersionsResponseData input = apiVersionsResponseData(apiVersion(ApiKeys.PRODUCE, (short) 1, (short) 6), apiVersion(ApiKeys.FETCH, (short) 1, (short) 7));
        ApiVersionsResponseData apiVersionsResponseData = interceptor.onApiVersionsResponse(input);
        assertVersionsForApiKeyNotPresent(apiVersionsResponseData, ApiKeys.PRODUCE);
        assertVersionsForApiKeyNotPresent(apiVersionsResponseData, ApiKeys.FETCH);
    }

}
