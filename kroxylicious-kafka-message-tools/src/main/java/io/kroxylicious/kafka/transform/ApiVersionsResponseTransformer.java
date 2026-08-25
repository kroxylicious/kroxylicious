/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kafka.transform;

import java.util.Objects;

import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;

import io.kroxylicious.proxy.tag.ThreadSafe;

/**
 * synchronously transforms ApiVersionsResponse data.
 * Implementations are expected to be ThreadSafe, such that a single transformer
 * instance is safe to be shared across threads.
 */
@ThreadSafe
public interface ApiVersionsResponseTransformer {
    /**
     * Transforms the given ApiVersionsResponse data.
     *
     * @param data data
     * @return data, may be the same object passed to the method, or a new instance
     * @throws ApiVersionsTransformationException if there are any problems
     */
    ApiVersionsResponseData transform(ApiVersionsResponseData data);

    /**
     * Composes this transformer with another, returning a transformer that applies
     * this transformer first and then applies the given {@code transformer} to the result.
     *
     * @param transformer the transformer to apply after this one, must not be null
     * @return the composed transformer
     */
    default ApiVersionsResponseTransformer and(ApiVersionsResponseTransformer transformer) {
        Objects.requireNonNull(transformer, "transformer must not be null");
        return data -> transformer.transform(transform(data));
    }
}
