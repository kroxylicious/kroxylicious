/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import java.util.Map;
import java.util.Set;

import io.kroxylicious.kafka.common.protocol.ApiKeys;

/**
 * Factory methods for common {@link ApiVersionsResponseTransformer} implementations.
 */
public class ApiVersionsResponseTransformers {
    private ApiVersionsResponseTransformers() {
    }

    /**
     * Creates a transformer that removes the given API keys from the ApiVersionsResponse.
     *
     * @param keysToRemove the API keys to remove
     * @return the transformer
     */
    public static ApiVersionsResponseTransformer removeApiKeys(Set<ApiKeys> keysToRemove) {
        return new ApiVersionRemover(keysToRemove);
    }

    /**
     * Creates a transformer that limits the maximum version advertised in the ApiVersionsResponse
     * for the given API keys.
     *
     * @param mapVersionLimits a map from API key to the maximum version to advertise for that key
     * @return the transformer
     */
    public static ApiVersionsResponseTransformer limitMaxVersionForApiKeys(Map<ApiKeys, Short> mapVersionLimits) {
        return new ApiVersionMaxVersionLimiter(mapVersionLimits);
    }

}
