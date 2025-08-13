/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A component for Filters that need to remove some RPCs. For instance if a new RPC is added that
 * breaks Filter behaviour, we can remove that RPC at version negotiation time so that the client
 * will not send it.
 */
class ApiVersionRemover implements ApiVersionsResponseTransformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiVersionRemover.class);

    private final Set<Short> apiIdsToRemove;

    ApiVersionRemover(Set<ApiKeys> keysToRemove) {
        Objects.requireNonNull(keysToRemove, "keysToRemove must not be null");
        this.apiIdsToRemove = keysToRemove.stream().map(apiKeys -> apiKeys.id).collect(Collectors.toSet());
    }

    @Override
    public ApiVersionsResponseData transform(ApiVersionsResponseData data) {
        if (!data.apiKeys().isEmpty()) {
            data.apiKeys().removeIf(x -> {
                boolean remove = apiIdsToRemove.contains(x.apiKey());
                if (remove) {
                    LOGGER.debug("Removing api versions for key {}", x.apiKey());
                }
                return remove;
            });
        }
        return data;
    }
}
