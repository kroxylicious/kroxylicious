/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.kroxylicious.proxy.ApiVersionsService;
import io.kroxylicious.proxy.filter.FilterContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;

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
    void testGetVersionRanges_SupportsFallbackToApiResponseV0() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        FilterContext filterContext = Mockito.mock(FilterContext.class);
        ApiVersionsResponseData unsupportedVersion = new ApiVersionsResponseData()
                .setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        Mockito.when(filterContext.sendRequest(argThat(header -> header != null && header.requestApiVersion() > 0), any()))
                .thenReturn(CompletableFuture.completedFuture(unsupportedVersion));
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, (short) (ApiKeys.METADATA.oldestVersion() - 1),
                (short) (ApiKeys.METADATA.latestVersion() - 1));
        Mockito.when(filterContext.sendRequest(argThat(header -> header != null && header.requestApiVersion() == 0), any()))
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
    void testGetVersionRanges_ApiVersionsRequestIsValid() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        FilterContext filterContext = Mockito.mock(FilterContext.class);
        ApiVersionsResponseData upstreamApiVersions = createApiVersionsWith(ApiKeys.METADATA.id, ApiKeys.METADATA.oldestVersion(),
                ApiKeys.METADATA.latestVersion());
        Mockito.when(filterContext.sendRequest(any(RequestHeaderData.class), any()))
                .thenReturn(CompletableFuture.completedFuture(upstreamApiVersions));
        apiVersionsService.getApiVersionRanges(ApiKeys.METADATA, filterContext).toCompletableFuture()
                .getNow(Optional.empty());
        ArgumentCaptor<RequestHeaderData> requestHeader = ArgumentCaptor.forClass(RequestHeaderData.class);
        ArgumentCaptor<ApiVersionsRequestData> requestData = ArgumentCaptor.forClass(ApiVersionsRequestData.class);
        Mockito.verify(filterContext).sendRequest(requestHeader.capture(), requestData.capture());
        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest(requestData.getValue(), requestHeader.getValue().requestApiVersion());
        assertThat(apiVersionsRequest.hasUnsupportedRequestVersion()).isFalse();
        assertThat(apiVersionsRequest.isValid()).isTrue();
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

    @Test
    void testSetApiVersionsRequest_SetAndGetApiVersionsRequest() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest(new ApiVersionsRequestData()
                .setClientSoftwareName("my-test-client")
                .setClientSoftwareVersion("1.0.0"), (short) 3);
        apiVersionsService.setApiVersionsRequest(apiVersionsRequest);
        assertThat(apiVersionsService.getApiVersionsRequest()).isSameAs(apiVersionsRequest);
    }

    @Test
    void testGetApiVersionsRequest_InvalidRequest() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();

        ApiVersionsRequest unsupportedApiVersion = new ApiVersionsRequest(new ApiVersionsRequestData()
                .setClientSoftwareName("my-test-client")
                .setClientSoftwareVersion("1.0.0"), (short) 0, (short) (ApiKeys.API_VERSIONS.latestVersion() + 1));
        assertThatThrownBy(() -> apiVersionsService.setApiVersionsRequest(unsupportedApiVersion))
                .isInstanceOf(UnsupportedVersionException.class);

        ApiVersionsRequest invalidClientSoftwareName = new ApiVersionsRequest(new ApiVersionsRequestData()
                .setClientSoftwareName("$my-test-client$")
                .setClientSoftwareVersion("1.0.0"), (short) 3);
        assertThatThrownBy(() -> apiVersionsService.setApiVersionsRequest(invalidClientSoftwareName))
                .isInstanceOf(InvalidRequestException.class);

        ApiVersionsRequest invalidClientSoftwareVersion = new ApiVersionsRequest(new ApiVersionsRequestData()
                .setClientSoftwareName("my-test-client")
                .setClientSoftwareVersion("$1.0.0$"), (short) 3);
        assertThatThrownBy(() -> apiVersionsService.setApiVersionsRequest(invalidClientSoftwareVersion))
                .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    void testGetApiVersionsRequest_DefaultApiVersionsRequest() {
        ApiVersionsServiceImpl apiVersionsService = new ApiVersionsServiceImpl();
        ApiVersionsRequest request = apiVersionsService.getApiVersionsRequest();
        assertThat(request.version()).isEqualTo(ApiKeys.API_VERSIONS.latestVersion());
        assertThat(request.data().clientSoftwareName()).isEqualTo("apache-kafka-java");
    }

}
