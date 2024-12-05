/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.NonNull;

public class ApiVersionsServiceImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiVersionsServiceImpl.class);
    private final Function<ApiKeys, Short> apiKeysShortFunction;

    public ApiVersionsServiceImpl() {
        this(Map.of());
    }

    public ApiVersionsServiceImpl(Map<ApiKeys, Short> overrideMap) {
        this(apiKeys -> getMaxApiVersionFor(apiKeys, overrideMap));
        sanityCheckOverrides(overrideMap);
    }

    private void sanityCheckOverrides(Map<ApiKeys, Short> overrideMap) {
        List<String> invalidEntries = overrideMap.entrySet().stream().flatMap(e -> validate(e.getKey(), e.getValue())).toList();
        if (!invalidEntries.isEmpty()) {
            Collector<CharSequence, ?, String> validationMessages = Collectors.joining(",", "[", "]");
            throw new IllegalArgumentException("api versions override map contained invalid entries: " + invalidEntries.stream().collect(validationMessages));
        }
    }

    private Stream<String> validate(ApiKeys key, Short overrideLatestVersion) {
        short oldestVersion = key.oldestVersion();
        if (overrideLatestVersion < oldestVersion) {
            return Stream.of(key.name() + " specified max version " + overrideLatestVersion + " less than oldest supported version " + key.oldestVersion());
        }
        return Stream.of();
    }

    private ApiVersionsServiceImpl(@NonNull Function<ApiKeys, Short> apiKeysShortFunction) {
        Objects.requireNonNull(apiKeysShortFunction);
        this.apiKeysShortFunction = apiKeysShortFunction;
    }

    private static short getMaxApiVersionFor(ApiKeys apiKey, Map<ApiKeys, Short> overrideMap) {
        short latest = apiKey.latestVersion(true);
        return Optional.ofNullable(overrideMap.get(apiKey)).map(m -> ((Integer) Math.min(latest, m.intValue())).shortValue()).orElse(latest);
    }

    public void updateVersions(String channel, ApiVersionsResponseData apiVersionsResponse) {
        intersectApiVersions(channel, apiVersionsResponse, apiKeysShortFunction);
    }

    private static void intersectApiVersions(String channel, ApiVersionsResponseData resp, Function<ApiKeys, Short> apiKeysShortFunction) {
        Set<ApiVersion> unknownApis = new HashSet<>();
        for (var key : resp.apiKeys()) {
            short apiId = key.apiKey();
            if (ApiKeys.hasId(apiId)) {
                ApiKeys apiKey = ApiKeys.forId(apiId);
                intersectApiVersion(channel, key, apiKey, apiKeysShortFunction);
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
     *
     * @param channel The channel.
     * @param key The key data from an upstream API_VERSIONS response.
     * @param apiKey The proxy's API key for this API.
     * @param apiKeysShortFunction
     */
    private static void intersectApiVersion(String channel, ApiVersion key, ApiKeys apiKey, Function<ApiKeys, Short> apiKeysShortFunction) {
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
                apiKeysShortFunction.apply(apiKey));
        if (mutualMax != key.maxVersion()) {
            LOGGER.trace("{}: {} max version changed to {} (was: {})", channel, apiKey, mutualMin, key.maxVersion());
            key.setMaxVersion(mutualMax);
        }
        else {
            LOGGER.trace("{}: {} max version unchanged (is: {})", channel, apiKey, mutualMin);
        }
    }

    public short latestVersion(ApiKeys apiKey) {
        return apiKeysShortFunction.apply(apiKey);
    }
}
