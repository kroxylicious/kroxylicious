/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

/**
 * Caches broker addresses from a METADATA response before node ID translation.
 */
@FunctionalInterface
interface MetadataAddressCacher {
    void cacheIfMetadata(Object responseBody);
}
