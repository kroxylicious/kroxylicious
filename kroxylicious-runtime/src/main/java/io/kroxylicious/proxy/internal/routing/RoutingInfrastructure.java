/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.function.IntUnaryOperator;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Timer;

import io.kroxylicious.proxy.service.HostPort;

/**
 * Per-connection routing state shared between {@link RouterDispatchHandler} and the
 * {@link RouterContextImpl} instances it creates for each request.
 *
 * <p>Some fields hold references to mutable objects ({@code sharedNodeAddresses},
 * {@code pendingResponseCount}, the supplier behind {@code routingCorrelationIdAllocator}).
 * This is intentional: the record groups per-connection state for convenient passing,
 * not as an immutability guarantee.</p>
 */
record RoutingInfrastructure(
                             Map<String, RouteDescriptor> routes,
                             NodeIdMapping nodeIdMapping,
                             Map<String, Integer> bootstrapVirtualNodeIds,
                             IntSupplier routingCorrelationIdAllocator,
                             MeterProvider<Counter> routingRequestsCounter,
                             MeterProvider<Counter> routingErrorsCounter,
                             MeterProvider<Timer> routingRequestDurationTimer,
                             AtomicInteger pendingResponseCount,
                             Map<Integer, HostPort> sharedNodeAddresses,
                             IntUnaryOperator virtualIdTranslator) {}
