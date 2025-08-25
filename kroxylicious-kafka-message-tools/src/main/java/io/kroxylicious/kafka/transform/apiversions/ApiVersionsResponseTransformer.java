/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform.apiversions;

import java.util.Objects;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.kafka.common.message.ApiVersionsResponseData;

/**
 * Intercepts and, optionally, synchronously transforms ApiVersionsResponse data.
 * Implementations are expected to be ThreadSafe, such that a single interceptor
 * instance is safe to be shared.
 */
@ThreadSafe
public interface ApiVersionsResponseTransformer {
    /**
     * @param data data
     * @return data, may be the same object passed to the method, or a new instance
     * @throws ApiVersionsInterceptionException if there are any problems
     */
    ApiVersionsResponseData onApiVersionsResponse(ApiVersionsResponseData data);

    default ApiVersionsResponseTransformer and(ApiVersionsResponseTransformer interceptor) {
        Objects.requireNonNull(interceptor, "interceptors must not be null");
        return data -> interceptor.onApiVersionsResponse(onApiVersionsResponse(data));
    }
}
