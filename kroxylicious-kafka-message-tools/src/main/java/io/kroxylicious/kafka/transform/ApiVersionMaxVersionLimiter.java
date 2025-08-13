/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A component for Filters that need to restrict the max version of a set of RPCs. For instance if
 * a new version of the Kafka RPC is incompatible with the Filter, and we wish to prevent the new version
 * being sent by the client
 */
class ApiVersionMaxVersionLimiter implements ApiVersionsResponseTransformer {

    private static final Logger logger = LoggerFactory.getLogger(ApiVersionMaxVersionLimiter.class);

    private final Map<ApiKeys, Short> versionLimits;

    ApiVersionMaxVersionLimiter(Map<ApiKeys, Short> apiKeyMaxVersionLimits) {
        Objects.requireNonNull(apiKeyMaxVersionLimits, "apiKeyMaxVersionLimits must not be null");
        this.versionLimits = new EnumMap<>(apiKeyMaxVersionLimits);
    }

    @Override
    public ApiVersionsResponseData transform(ApiVersionsResponseData data) {
        if (!data.apiKeys().isEmpty()) {
            for (Map.Entry<ApiKeys, Short> apiKeyLimit : versionLimits.entrySet()) {
                ApiVersionsResponseData.ApiVersion version = data.apiKeys().find(apiKeyLimit.getKey().id);
                if (version != null) {
                    Short limit = apiKeyLimit.getValue();
                    if (version.minVersion() > limit) {
                        throw new ApiVersionsTransformationException(
                                "upstream advertised min version " + version.minVersion() + " for " + apiKeyLimit.getKey().name() + " is above the limit of "
                                        + limit + " imposed by this transformer");
                    }
                    if (version.maxVersion() > limit) {
                        logger.debug("Downgrading max version {} for {} to {}", version.maxVersion(), apiKeyLimit.getKey().name(), limit);
                        version.setMaxVersion(limit);
                    }
                }
            }
        }
        return data;
    }
}
