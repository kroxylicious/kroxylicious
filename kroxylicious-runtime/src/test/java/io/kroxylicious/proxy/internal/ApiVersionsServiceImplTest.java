/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Map;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiVersionsServiceImplTest {

    private ApiVersionsServiceImpl apiVersionsService;

    @BeforeEach
    void setUp() {
        apiVersionsService = new ApiVersionsServiceImpl();
    }

    @Test
    void testIntersection_UpstreamMatchesApiKeys() {
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, ApiKeys.METADATA.oldestVersion(),
                ApiKeys.METADATA.latestVersion());
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
    }

    @Test
    void testIntersection_UpstreamMinVersionLessThanApiKeys() {
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, (short) (ApiKeys.METADATA.oldestVersion() - 1),
                ApiKeys.METADATA.latestVersion());
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
    }

    @Test
    void shouldMarkProduceRequestV0AsSupported() {
        // Given
        short oldestProduceRequest = ApiKeys.PRODUCE.messageType.lowestDeprecatedVersion();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.PRODUCE.id, oldestProduceRequest, ApiKeys.PRODUCE.latestVersion());

        // When
        apiVersionsService.updateVersions("channel", upstreamApiVersions);

        // Then
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.PRODUCE, oldestProduceRequest, ApiKeys.PRODUCE.latestVersion());
    }

    @Test
    void testIntersection_UpstreamMinVersionGreaterThanApiKeys() {
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, (short) (ApiKeys.METADATA.oldestVersion() + 1),
                ApiKeys.METADATA.latestVersion());
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, (short) (ApiKeys.METADATA.oldestVersion() + 1), ApiKeys.METADATA.latestVersion());
    }

    @Test
    void testIntersection_UpstreamMaxVersionLessThanApiKeys() {
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, ApiKeys.METADATA.oldestVersion(),
                (short) (ApiKeys.METADATA.latestVersion() - 1));
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), (short) (ApiKeys.METADATA.latestVersion() - 1));
    }

    @Test
    void testIntersection_UpstreamMaxVersionGreaterThanApiKeys() {
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, ApiKeys.METADATA.oldestVersion(),
                (short) (ApiKeys.METADATA.latestVersion() + 1));
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
    }

    @Test
    void testIntersection_DiscardsUnknownUpstreamApiKeys() {
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith((short) 678, ApiKeys.METADATA.oldestVersion(),
                (short) (ApiKeys.METADATA.latestVersion() + 1));
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThat(upstreamApiVersions.apiKeys()).isEmpty();
    }

    @Test
    void intersectionConsidersMaxVersionOverride() {
        short overrideMaxVersion = (short) (ApiKeys.METADATA.latestVersion(true) - 1);
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl(Map.of(ApiKeys.METADATA, overrideMaxVersion));
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, ApiKeys.METADATA.oldestVersion(),
                (short) (ApiKeys.METADATA.latestVersion() + 1));
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), overrideMaxVersion);
    }

    @Test
    void latestVersion() {
        assertThat(apiVersionsService.latestVersion(ApiKeys.API_VERSIONS)).isEqualTo(ApiKeys.API_VERSIONS.latestVersion(true));
    }

    @Test
    void latestVersionConsidersMaxVersionOverride() {
        short overrideMaxVersion = (short) (ApiKeys.API_VERSIONS.latestVersion(true) - 1);
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl(Map.of(ApiKeys.API_VERSIONS, overrideMaxVersion));
        assertThat(apiVersionsService.latestVersion(ApiKeys.API_VERSIONS)).isEqualTo(overrideMaxVersion);
    }

    @Test
    void versionOverridesMustBeAboveTheOldestVersionForEachKey() {
        short overrideMaxVersion = (short) (ApiKeys.API_VERSIONS.oldestVersion() - 1);
        Map<ApiKeys, Short> overrideMap = Map.of(ApiKeys.API_VERSIONS, overrideMaxVersion);
        assertThatThrownBy(() -> new ApiVersionsServiceImpl(overrideMap)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("api versions override map contained invalid entries: [API_VERSIONS specified max version -1 less than oldest supported version 0]");
    }

    private static void assertThatApiVersionsContainsExactly(ApiVersionsResponseData upstreamApiVersions, ApiKeys apiKeys, short minVersion, short maxVersion) {
        assertThat(upstreamApiVersions.apiKeys()).satisfies(apiVersions -> assertThat(apiVersions).hasSize(1).first().satisfies(apiVersion -> {
            assertThat(apiVersion.apiKey()).isEqualTo(apiKeys.id);
            assertThat(apiVersion.minVersion()).isEqualTo(minVersion);
            assertThat(apiVersion.maxVersion()).isEqualTo(maxVersion);
        }));
    }

    private static ApiVersionsResponseData createApiVersionsWith(short api, short minVersion, short maxVersion) {
        ApiVersionsResponseData upstreamApiVersions = new ApiVersionsResponseData();
        upstreamApiVersions.apiKeys().add(new ApiVersionsResponseData.ApiVersion().setApiKey(api).setMinVersion(minVersion).setMaxVersion(
                maxVersion));
        return upstreamApiVersions;
    }

}
