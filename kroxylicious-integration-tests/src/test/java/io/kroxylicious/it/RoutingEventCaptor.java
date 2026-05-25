/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.proxy.internal.routing.RoutingEvent;

/**
 * Test utility that captures {@link RoutingEvent}s as requests flow
 * through the routing layer. Implements {@link AutoCloseable} so
 * tests can use try-with-resources.
 */
public class RoutingEventCaptor implements AutoCloseable {

    private final List<RoutingEvent> events = new CopyOnWriteArrayList<>();

    private RoutingEventCaptor() {
    }

    /**
     * Installs a global event listener and returns a captor that
     * collects all routing events until {@link #close()} is called.
     */
    public static RoutingEventCaptor install() {
        var captor = new RoutingEventCaptor();
        RoutingEvent.setEventListener(captor.events::add);
        return captor;
    }

    @Override
    public void close() {
        RoutingEvent.setEventListener(null);
    }

    /** All captured events (requests and responses). */
    public List<RoutingEvent> events() {
        return List.copyOf(events);
    }

    /** All captured request events. */
    public List<RoutingEvent.Request> requestEvents() {
        return events.stream()
                .filter(RoutingEvent.Request.class::isInstance)
                .map(RoutingEvent.Request.class::cast)
                .toList();
    }

    /** All captured response events. */
    public List<RoutingEvent.Response> responseEvents() {
        return events.stream()
                .filter(RoutingEvent.Response.class::isInstance)
                .map(RoutingEvent.Response.class::cast)
                .toList();
    }

    /** Request events sent to the given route. */
    public List<RoutingEvent.Request> requestsToRoute(String route) {
        return requestEvents().stream()
                .filter(e -> e.route().equals(route))
                .toList();
    }

    /** Request events sent to the given route for the given API key. */
    public List<RoutingEvent.Request> requestsToRoute(String route,
                                                      ApiKeys apiKey) {
        return requestEvents().stream()
                .filter(e -> e.route().equals(route))
                .filter(e -> e.apiKey() == apiKey)
                .toList();
    }
}
