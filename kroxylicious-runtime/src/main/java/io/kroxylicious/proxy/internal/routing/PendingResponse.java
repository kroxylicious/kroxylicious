/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.protocol.ApiKeys;

import io.micrometer.core.instrument.Timer;

import io.kroxylicious.proxy.router.Response;

record PendingResponse(CompletableFuture<Response> future,
                       Timer.Sample timerSample,
                       String route,
                       ApiKeys apiKey,
                       NodeIdMapping nodeIdMapping,
                       MetadataAddressCacher metadataAddressCacher) {}
