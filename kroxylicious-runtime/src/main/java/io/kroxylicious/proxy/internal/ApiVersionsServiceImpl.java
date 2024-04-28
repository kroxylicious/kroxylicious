/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.ApiVersionsService;
import io.kroxylicious.proxy.filter.FilterContext;

public class ApiVersionsServiceImpl {

    private record ApiVersions(ApiVersionCollection upstream, ApiVersionCollection intersected) {}

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiVersionsServiceImpl.class);
    private ApiVersions apiVersions = null;
    private ApiVersionsRequest apiVersionsRequest = null;

    public void updateVersions(String channel, ApiVersionsResponseData upstreamApiVersions) {
        var upstream = upstreamApiVersions.duplicate().apiKeys();
        intersectApiVersions(channel, upstreamApiVersions);
        var intersected = upstreamApiVersions.duplicate().apiKeys();
        this.apiVersions = new ApiVersions(upstream, intersected);
    }

    private static void intersectApiVersions(String channel, ApiVersionsResponseData resp) {
        Set<ApiVersion> unknownApis = new HashSet<>();
        for (var key : resp.apiKeys()) {
            short apiId = key.apiKey();
            if (ApiKeys.hasId(apiId)) {
                ApiKeys apiKey = ApiKeys.forId(apiId);
                intersectApiVersion(channel, key, apiKey);
            }
            else {
                unknownApis.add(key);
            }
        }
        resp.apiKeys().removeAll(unknownApis);
    }

    /**
     * Update the given {@code key}'s max and min versions so that the client uses APIs versions mutually
     * understood by both the proxy and the broker.
     * @param channel The channel.
     * @param key The key data from an upstream API_VERSIONS response.
     * @param apiKey The proxy's API key for this API.
     */
    private static void intersectApiVersion(String channel, ApiVersionsResponseData.ApiVersion key, ApiKeys apiKey) {
        short mutualMin = (short) Math.max(
                key.minVersion(),
                apiKey.messageType.lowestSupportedVersion());
        if (mutualMin != key.minVersion()) {
            LOGGER.trace("{}: {} min version changed to {} (was: {})", channel, apiKey, mutualMin, key.maxVersion());
            key.setMinVersion(mutualMin);
        }
        else {
            LOGGER.trace("{}: {} min version unchanged (is: {})", channel, apiKey, mutualMin);
        }

        short mutualMax = (short) Math.min(
                key.maxVersion(),
                apiKey.messageType.highestSupportedVersion(true));
        if (mutualMax != key.maxVersion()) {
            LOGGER.trace("{}: {} max version changed to {} (was: {})", channel, apiKey, mutualMin, key.maxVersion());
            key.setMaxVersion(mutualMax);
        }
        else {
            LOGGER.trace("{}: {} max version unchanged (is: {})", channel, apiKey, mutualMin);
        }
    }

    public CompletionStage<Optional<ApiVersionsService.ApiVersionRanges>> getApiVersionRanges(ApiKeys keys, FilterContext context) {
        return getVersions(context).thenApply(versions -> {
            ApiVersion upstream = versions.upstream.find(keys.id);
            ApiVersion intersected = versions.intersected.find(keys.id);
            if (upstream == null || intersected == null) {
                return Optional.empty();
            }
            else {
                return Optional.of(new ApiVersionsService.ApiVersionRanges(upstream, intersected));
            }
        });
    }

    private CompletionStage<ApiVersions> getVersions(FilterContext context) {
        if (apiVersions != null) {
            return CompletableFuture.completedFuture(apiVersions);
        }

        // KIP-511 when the client receives an unsupported version for the ApiVersionResponse, it fails back to version 0
        // Use the same algorithm as https://github.com/apache/kafka/blob/159d25a7df25975694e2e0eb18a8feb125f7c39e/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L957-L977
        var request = getApiVersionsRequest();
        var header = new RequestHeaderData().setRequestApiVersion(request.version());
        return context.<ApiVersionsResponseData> sendRequest(header, request.data())
                .thenCompose(response -> {
                    if (response.errorCode() != Errors.NONE.code()) {
                        if (header.requestApiVersion() == 0 || response.errorCode() != Errors.UNSUPPORTED_VERSION.code()) {
                            throw new IllegalStateException("Received error " + Errors.forCode(response.errorCode()) +
                                    " when making an ApiVersionsRequest with correlation id " + header.correlationId() + ".");
                        }

                        short maxApiVersion = 0;
                        if (!response.apiKeys().isEmpty()) {
                            ApiVersion apiVersion = response.apiKeys().find(ApiKeys.API_VERSIONS.id);
                            if (apiVersion != null) {
                                maxApiVersion = apiVersion.maxVersion();
                            }
                        }
                        return context.sendRequest(header.setRequestApiVersion(maxApiVersion), request.data());
                    }
                    return CompletableFuture.completedStage(response);
                })
                .thenApply(response -> {
                    updateVersions(context.channelDescriptor(), response);
                    return apiVersions;
                });
    }

    void setApiVersionsRequest(ApiVersionsRequest request) {
        if (request.hasUnsupportedRequestVersion()) {
            throw Errors.UNSUPPORTED_VERSION.exception();
        }
        if (!request.isValid()) {
            throw Errors.INVALID_REQUEST.exception();
        }
        this.apiVersionsRequest = request;
    }

    ApiVersionsRequest getApiVersionsRequest() {
        if (apiVersionsRequest == null) {
            ApiVersionsRequest.Builder builder = new ApiVersionsRequest.Builder();
            short version = builder.latestAllowedVersion();
            apiVersionsRequest = builder.build(version);
        }
        return apiVersionsRequest;
    }

}
