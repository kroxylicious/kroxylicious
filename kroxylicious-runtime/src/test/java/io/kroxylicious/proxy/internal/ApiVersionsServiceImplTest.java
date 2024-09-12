/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ApiVersionsServiceImplTest {

    @Test
    void testIntersection_UpstreamMatchesApiKeys() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(
                ApiKeys.METADATA.id,
                ApiKeys.METADATA.oldestVersion(),
                ApiKeys.METADATA.latestVersion()
        );
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
    }

    @Test
    void testIntersection_UpstreamMinVersionLessThanApiKeys() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(
                ApiKeys.METADATA.id,
                (short) (ApiKeys.METADATA.oldestVersion() - 1),
                ApiKeys.METADATA.latestVersion()
        );
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
    }

    @Test
    void testIntersection_UpstreamMinVersionGreaterThanApiKeys() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(
                ApiKeys.METADATA.id,
                (short) (ApiKeys.METADATA.oldestVersion() + 1),
                ApiKeys.METADATA.latestVersion()
        );
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, (short) (ApiKeys.METADATA.oldestVersion() + 1), ApiKeys.METADATA.latestVersion());
    }

    @Test
    void testIntersection_UpstreamMaxVersionLessThanApiKeys() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(
                ApiKeys.METADATA.id,
                ApiKeys.METADATA.oldestVersion(),
                (short) (ApiKeys.METADATA.latestVersion() - 1)
        );
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), (short) (ApiKeys.METADATA.latestVersion() - 1));
    }

    @Test
    void testIntersection_UpstreamMaxVersionGreaterThanApiKeys() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(
                ApiKeys.METADATA.id,
                ApiKeys.METADATA.oldestVersion(),
                (short) (ApiKeys.METADATA.latestVersion() + 1)
        );
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
    }

    @Test
    void testIntersection_DiscardsUnknownUpstreamApiKeys() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(
                (short) 678,
                ApiKeys.METADATA.oldestVersion(),
                (short) (ApiKeys.METADATA.latestVersion() + 1)
        );
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThat(upstreamApiVersions.apiKeys()).isEmpty();
    }

    private static void assertThatApiVersionsContainsExactly(ApiVersionsResponseData upstreamApiVersions, ApiKeys apiKeys, short minVersion, short maxVersion) {
        assertThat(upstreamApiVersions.apiKeys()).satisfies(apiVersions -> {
            assertThat(apiVersions).hasSize(1).first().satisfies(apiVersion -> {
                assertThat(apiVersion.apiKey()).isEqualTo(apiKeys.id);
                assertThat(apiVersion.minVersion()).isEqualTo(minVersion);
                assertThat(apiVersion.maxVersion()).isEqualTo(maxVersion);
            });
        });
    }

    private static ApiVersionsResponseData createApiVersionsWith(short api, short minVersion, short maxVersion) {
        ApiVersionsResponseData upstreamApiVersions = new ApiVersionsResponseData();
        upstreamApiVersions.apiKeys()
                           .add(
                                   new ApiVersionsResponseData.ApiVersion().setApiKey(api)
                                                                           .setMinVersion(minVersion)
                                                                           .setMaxVersion(
                                                                                   maxVersion
                                                                           )
                           );
        return upstreamApiVersions;
    }

}
