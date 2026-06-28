/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.protocol.ApiKeys;

public class ApiVersionsResponseTransformers {
    private ApiVersionsResponseTransformers() {
    }

    public static ApiVersionsResponseTransformer removeApiKeys(Set<ApiKeys> keysToRemove) {
        return new ApiVersionRemover(keysToRemove);
    }

    public static ApiVersionsResponseTransformer limitMaxVersionForApiKeys(Map<ApiKeys, Short> mapVersionLimits) {
        return new ApiVersionMaxVersionLimiter(mapVersionLimits);
    }

}
