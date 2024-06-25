/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metadata;

import java.util.concurrent.CompletableFuture;

import io.kroxylicious.proxy.metadata.ResourceMetadataRequest;
import io.kroxylicious.proxy.metadata.ResourceMetadataResponse;

public record ResourceMetadataFrame<Q extends ResourceMetadataRequest<R>, R extends ResourceMetadataResponse<Q>>(Q request,
                                                                                                                 CompletableFuture<R> promise) {}
