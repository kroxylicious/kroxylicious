/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import java.util.Arrays;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;

import static org.assertj.core.api.Assertions.assertThat;

public class ApiVersionsResponseTransformerTester {
    protected static ApiVersionsResponseData apiVersionsResponseData(ApiVersionsResponseData.ApiVersion... versions) {
        ApiVersionsResponseData apiVersionsResponseData = new ApiVersionsResponseData();
        apiVersionsResponseData.apiKeys().addAll(Arrays.asList(versions));
        return apiVersionsResponseData;
    }

    protected static ApiVersionsResponseData.ApiVersion apiVersion(ApiKeys apiKeys, short minVersoin, short maxVersion) {
        return new ApiVersionsResponseData.ApiVersion().setApiKey(apiKeys.id).setMinVersion(minVersoin).setMaxVersion(maxVersion);
    }

    protected static void assertVersionsForApiKey(ApiVersionsResponseData apiVersionsResponseData, ApiKeys apiKeys, short min, short max) {
        assertThat(apiVersionsResponseData).isNotNull().satisfies(apiVersionsResponse -> {
            ApiVersionsResponseData.ApiVersion version = apiVersionsResponse.apiKeys().find(apiKeys.id);
            assertThat(version).isNotNull().satisfies(apiVersionResponse -> {
                assertThat(apiVersionResponse.minVersion()).isEqualTo(min);
                assertThat(apiVersionResponse.maxVersion()).isEqualTo(max);
            });
        });
    }

    protected static void assertVersionsForApiKeyNotPresent(ApiVersionsResponseData apiVersionsResponseData, ApiKeys apiKeys) {
        assertThat(apiVersionsResponseData).isNotNull().satisfies(apiVersionsResponse -> {
            ApiVersionsResponseData.ApiVersion version = apiVersionsResponse.apiKeys().find(apiKeys.id);
            assertThat(version).isNull();
        });
    }
}
