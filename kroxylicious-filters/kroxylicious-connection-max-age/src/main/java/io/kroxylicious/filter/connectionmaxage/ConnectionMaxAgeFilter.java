/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.connectionmaxage;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * A filter that closes client connections after a configurable maximum age.
 * <p>
 * This is useful in environments like Kubernetes where proxy instances scale up and down.
 * Long-lived client connections can become stuck on old proxy instances, causing connection
 * skew. By closing connections after a maximum age, clients will reconnect and be balanced
 * across the available proxy instances.
 * <p>
 * Each connection gets its own effective max age deadline, computed at filter creation time.
 * When jitter is configured, the effective deadline is randomized within
 * {@code [maxAge - jitter, maxAge + jitter]} to avoid thundering herd reconnection storms.
 * <p>
 * On the next request after the deadline has passed, the filter forwards the request but
 * signals that the connection should be closed afterwards.
 */
public class ConnectionMaxAgeFilter implements RequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionMaxAgeFilter.class);

    private final Instant deadline;
    private final Clock clock;

    ConnectionMaxAgeFilter(Duration effectiveMaxAge, Clock clock) {
        this.clock = clock;
        this.deadline = clock.instant().plus(effectiveMaxAge);
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          short apiVersion,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext context) {
        if (clock.instant().isAfter(deadline)) {
            LOGGER.atInfo()
                    .setMessage("Connection max age exceeded, closing after forwarding request")
                    .addKeyValue("channel", context.channelDescriptor())
                    .addKeyValue("deadline", deadline)
                    .addKeyValue("apiKey", apiKey)
                    .log();
            return context.requestFilterResultBuilder()
                    .forward(header, request)
                    .withCloseConnection()
                    .completed();
        }
        return context.forwardRequest(header, request);
    }
}
