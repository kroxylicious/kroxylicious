/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.audit;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import edu.umd.cs.findbugs.annotations.NonNull;

import edu.umd.cs.findbugs.annotations.Nullable;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

public class AuditFilter implements RequestFilter, ResponseFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuditFilter.class);
    private final Audit.Config configuration;
    private final EventSink sink;

    private final Queue<CompletableFuture<ApiMessage>> pendingResponses = new ArrayDeque<>();

    public AuditFilter(Audit.Config configuration, EventSink sink) {
        this.configuration = Objects.requireNonNull(configuration);
        this.sink = sink;
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return configuration.apiKeys().contains(apiKey);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return configuration.apiKeys().contains(apiKey);
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        var responseFuture = new CompletableFuture<ApiMessage>();
        var after = responseFuture.thenAccept(response -> sinkEvents(context, request, response));
        if (request instanceof ProduceRequestData prd && prd.acks() == 0) {
            // If there's no going to be a response, chain the completion of to the last pendingResponse (if any),
            // so that audit log ordering is maintained.
            var peek = pendingResponses.peek();
            if (peek != null) {
                peek.thenRun(() -> responseFuture.complete(null));
            }
            else {
                after.complete(null);
            }
        }
        else {
            pendingResponses.add(responseFuture);
        }
        return context.forwardRequest(header, request);
    }


    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {
        var responseFuture = pendingResponses.remove();
        responseFuture.complete(response);
        return context.forwardResponse(header, response);
    }

    private void sinkEvents(FilterContext context, @NonNull ApiMessage request, @Nullable ApiMessage response) {
        // TODO: turn this into a event factory
        // TODO: avoid class cast inefficiency
        // TODO: could we generate the code?

        var events = new ArrayList<Event>();

        var apiKey = ApiKeys.forId(request.apiKey());

        if (request instanceof CreateTopicsRequestData ctrqd && response instanceof CreateTopicsResponseData ctrsd) {
            if (ctrqd.topics().size() != ctrsd.topics().size()) {
                throw new IllegalStateException();
            }

            var rqItr = ctrqd.topics().iterator();
            var rsItr = ctrsd.topics().iterator();
            while (rqItr.hasNext()) {
                var rqTopic = rqItr.next();
                var rsTopic = rsItr.next();
                if (rqTopic.name().equals(rsTopic.name())) {
                    throw new IllegalStateException();
                }
                // TODO add IP address of peer, possibly discovered by the Proxy Protocol
                // TODO add userid, if known
                var event = new Event(UUID.randomUUID(), apiKey, List.of(new Entity(Entity.EntityType.TOPIC_NAME, rqTopic.name())), rsTopic.errorCode());
                events.add(event);
            }
        }
        sink.acceptEvents(events);

    }


}
