/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.OptionalInt;
import java.util.OptionalLong;

import org.apache.kafka.common.protocol.ApiKeys;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Condition;

import io.kroxylicious.proxy.internal.routing.RoutingEvent;

/**
 * AssertJ custom assert for {@link RoutingEvent.Request}, providing
 * domain-specific assertions for route selection, correlation IDs,
 * and produce-specific metadata (producer IDs, sequence numbers).
 */
public class RoutingRequestEventAssert
        extends AbstractAssert<RoutingRequestEventAssert, RoutingEvent.Request> {

    private RoutingRequestEventAssert(RoutingEvent.Request actual) {
        super(actual, RoutingRequestEventAssert.class);
    }

    public static RoutingRequestEventAssert assertThat(RoutingEvent.Request actual) {
        return new RoutingRequestEventAssert(actual);
    }

    public RoutingRequestEventAssert hasRoute(String expected) {
        isNotNull();
        if (!actual.route().equals(expected)) {
            failWithMessage("Expected route <%s> but was <%s>",
                    expected, actual.route());
        }
        return this;
    }

    public RoutingRequestEventAssert hasApiKey(ApiKeys expected) {
        isNotNull();
        if (actual.apiKey() != expected) {
            failWithMessage("Expected apiKey <%s> but was <%s>",
                    expected, actual.apiKey());
        }
        return this;
    }

    public RoutingRequestEventAssert hasClientCorrelationId(int expected) {
        isNotNull();
        if (actual.clientCorrelationId() != expected) {
            failWithMessage("Expected clientCorrelationId <%d> but was <%d>",
                    expected, actual.clientCorrelationId());
        }
        return this;
    }

    public RoutingRequestEventAssert hasNegativeRoutingCorrelationId() {
        isNotNull();
        if (actual.routingCorrelationId() >= 0) {
            failWithMessage("Expected negative routingCorrelationId but was <%d>",
                    actual.routingCorrelationId());
        }
        return this;
    }

    public RoutingRequestEventAssert hasProducerId(long expected) {
        isNotNull();
        OptionalLong pid = actual.firstProducerId();
        if (pid.isEmpty()) {
            failWithMessage("Expected producerId <%d> but no idempotent batch found", expected);
        }
        else if (pid.getAsLong() != expected) {
            failWithMessage("Expected producerId <%d> but was <%d>",
                    expected, pid.getAsLong());
        }
        return this;
    }

    public RoutingRequestEventAssert hasBaseSequence(int expected) {
        isNotNull();
        OptionalInt seq = actual.firstBaseSequence();
        if (seq.isEmpty()) {
            failWithMessage("Expected baseSequence <%d> but no idempotent batch found", expected);
        }
        else if (seq.getAsInt() != expected) {
            failWithMessage("Expected baseSequence <%d> but was <%d>",
                    expected, seq.getAsInt());
        }
        return this;
    }

    public RoutingRequestEventAssert hasProducerEpoch(int expected) {
        isNotNull();
        OptionalInt epoch = actual.firstProducerEpoch();
        if (epoch.isEmpty()) {
            failWithMessage("Expected producerEpoch <%d> but no idempotent batch found", expected);
        }
        else if (epoch.getAsInt() != expected) {
            failWithMessage("Expected producerEpoch <%d> but was <%d>",
                    expected, epoch.getAsInt());
        }
        return this;
    }

    /** Condition: event targets the given route. */
    public static Condition<RoutingEvent.Request> routeIs(String route) {
        return new Condition<>(e -> e.route().equals(route), "route is '%s'", route);
    }

    /** Condition: event has the given API key. */
    public static Condition<RoutingEvent.Request> apiKeyIs(ApiKeys apiKey) {
        return new Condition<>(e -> e.apiKey() == apiKey, "apiKey is %s", apiKey);
    }
}
