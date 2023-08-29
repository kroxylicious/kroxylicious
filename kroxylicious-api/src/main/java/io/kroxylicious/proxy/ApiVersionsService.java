/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.protocol.ApiKeys;

import edu.umd.cs.findbugs.annotations.Nullable;

public interface ApiVersionsService {

    /**
     * Information about version ranges for an ApiKey supported by the upstream and Kroxylicious
     * @param upstream the version range supported by the upstream server, or null if this key is not supported by upstream
     * @param intersected the version range supported by both Kroxylicious and the upstream server, or null if this ApiKey is not supported
     */
    record ApiVersionRanges(@Nullable ApiVersion upstream, @Nullable ApiVersion intersected) {
    }

    /**
     * Get the supported version ranges for an ApiKey. Will contain the upstream supported
     * version range, and the intersected version range supported by the proxy and upstream.
     * Filters will likely want to work with the intersected range as both the proxy and the
     * upstream can parse those versions.
     * @param keys keys
     * @return a CompletionStage that will be completed with the upstream ApiVersionRanges
     */
    CompletionStage<ApiVersionRanges> getApiVersionRanges(ApiKeys keys);
}
