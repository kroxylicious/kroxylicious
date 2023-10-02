/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.ApiVersionsService;
import io.kroxylicious.proxy.filter.FilterContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

class ApiVersionsServiceImplTest {

    @Test
    void testGetVersionRanges_UsesCachedResultFromIntersectionMutation() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, (short) (ApiKeys.METADATA.oldestVersion() + 1),
                (short) (ApiKeys.METADATA.latestVersion() + 1));
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        ApiVersionsService.ApiVersionRanges range = apiVersionsService.getApiVersionRanges(ApiKeys.METADATA, Mockito.mock(FilterContext.class)).toCompletableFuture()
                .getNow(Optional.empty()).orElse(null);
        assertThat(range).isNotNull();
        assertThat(range.upstream().minVersion()).isEqualTo((short) (ApiKeys.METADATA.oldestVersion() + 1));
        assertThat(range.upstream().maxVersion()).isEqualTo((short) (ApiKeys.METADATA.latestVersion() + 1));
        assertThat(range.intersected().minVersion()).isEqualTo((short) (ApiKeys.METADATA.oldestVersion() + 1));
        assertThat(range.intersected().maxVersion()).isEqualTo(ApiKeys.METADATA.latestVersion());
    }

    @Test
    void testGetVersionRanges_UsesFilterContextToObtainVersionsIfRequired() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, (short) (ApiKeys.METADATA.oldestVersion() - 1),
                (short) (ApiKeys.METADATA.latestVersion() - 1));
        FilterContext filterContext = Mockito.mock(FilterContext.class);
        Mockito.when(filterContext.sendRequest(any(RequestHeaderData.class), any()))
                .thenReturn(CompletableFuture.completedFuture(upstreamApiVersions));
        ApiVersionsService.ApiVersionRanges range = apiVersionsService.getApiVersionRanges(ApiKeys.METADATA, filterContext).toCompletableFuture()
                .getNow(Optional.empty()).orElse(null);
        assertThat(range).isNotNull();
        assertThat(range.upstream().minVersion()).isEqualTo((short) (ApiKeys.METADATA.oldestVersion() - 1));
        assertThat(range.upstream().maxVersion()).isEqualTo((short) (ApiKeys.METADATA.latestVersion() - 1));
        assertThat(range.intersected().minVersion()).isEqualTo(ApiKeys.METADATA.oldestVersion());
        assertThat(range.intersected().maxVersion()).isEqualTo((short) (ApiKeys.METADATA.latestVersion() - 1));
    }

    @Test
    void testIntersection_UpstreamMatchesApiKeys() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, ApiKeys.METADATA.oldestVersion(),
                ApiKeys.METADATA.latestVersion());
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
    }

    @Test
    void testIntersection_UpstreamMinVersionLessThanApiKeys() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, (short) (ApiKeys.METADATA.oldestVersion() - 1),
                ApiKeys.METADATA.latestVersion());
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
    }

    @Test
    void testIntersection_UpstreamMinVersionGreaterThanApiKeys() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, (short) (ApiKeys.METADATA.oldestVersion() + 1),
                ApiKeys.METADATA.latestVersion());
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, (short) (ApiKeys.METADATA.oldestVersion() + 1), ApiKeys.METADATA.latestVersion());
    }

    @Test
    void testIntersection_UpstreamMaxVersionLessThanApiKeys() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, ApiKeys.METADATA.oldestVersion(),
                (short) (ApiKeys.METADATA.latestVersion() - 1));
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), (short) (ApiKeys.METADATA.latestVersion() - 1));
    }

    @Test
    void testIntersection_UpstreamMaxVersionGreaterThanApiKeys() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, ApiKeys.METADATA.oldestVersion(),
                (short) (ApiKeys.METADATA.latestVersion() + 1));
        apiVersionsService.updateVersions("channel", upstreamApiVersions);
        assertThatApiVersionsContainsExactly(upstreamApiVersions, ApiKeys.METADATA, ApiKeys.METADATA.oldestVersion(), ApiKeys.METADATA.latestVersion());
    }

    @Test
    void testIntersection_DiscardsUnknownUpstreamApiKeys() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith((short) 678, ApiKeys.METADATA.oldestVersion(),
                (short) (ApiKeys.METADATA.latestVersion() + 1));
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
        upstreamApiVersions.apiKeys().add(new ApiVersionsResponseData.ApiVersion().setApiKey(api).setMinVersion(minVersion).setMaxVersion(
                maxVersion));
        return upstreamApiVersions;
    }

}
