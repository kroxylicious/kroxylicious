/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.micrometer.core.instrument.Timer;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * @param coordinatorContext request-side context for FIND_COORDINATOR requests,
 *                           or null for other API keys. Carries the key type and key
 *                           from the request so the response can be cached in the
 *                           TopologyCache (the response itself lacks keyType, and
 *                           pre-v4 responses lack key).
 */
record PendingResponse(CompletableFuture<ApiMessage> future,
                       Timer.Sample timerSample,
                       String route,
                       ApiKeys apiKey,
                       NodeIdMapping nodeIdMapping,
                       MetadataAddressCacher metadataAddressCacher,
                       @Nullable CoordinatorRequestContext coordinatorContext) {

    record CoordinatorRequestContext(byte keyType, String key) {}
}
