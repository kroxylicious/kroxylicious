/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.BaseConfig;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A Filter that adds an UnknownTaggedField to request and/or response messages for some
 * specified set of ApiKeys.  It supports synchronous and asynchronous forwarding styles.
 * <br/>
 * The tags have values:
 * <ul>
 *     <li>Requests: RequestResponseMarkingFilter-${config.name}-request</li>
 *     <li>Responses: RequestResponseMarkingFilter-${config.name}-response</li>
 * </ul>
 */
public class RequestResponseMarkingFilter implements RequestFilter, ResponseFilter {

    public enum Direction {
        REQUEST,
        RESPONSE;
    }

    public enum ForwardingStyle implements BiFunction<KrpcFilterContext, ApiMessage, CompletionStage<ApiMessage>> {
        SYNCHRONOUS {
            @Override
            public CompletionStage<ApiMessage> apply(KrpcFilterContext context, ApiMessage body) {
                return CompletableFuture.completedStage(body);
            }
        },
        ASYNCHRONOUS_DELAYED {
            @Override
            public CompletionStage<ApiMessage> apply(KrpcFilterContext context, ApiMessage body) {
                CompletableFuture<ApiMessage> result = new CompletableFuture<>();
                try (var executor = Executors.newScheduledThreadPool(1)) {
                    var delay = (long) (Math.random() * 200);
                    executor.schedule(() -> {
                        result.complete(body);
                    }, delay, TimeUnit.MILLISECONDS);
                }
                return result;
            }
        },
        ASYNCHRONOUS_REQUEST_TO_BROKER {
            @Override
            public CompletionStage<ApiMessage> apply(KrpcFilterContext context, ApiMessage body) {
                return sendAsyncRequestAndCheckForResponseErrors(context).thenApply(unused -> body);
            }

            private CompletionStage<ListGroupsResponseData> sendAsyncRequestAndCheckForResponseErrors(KrpcFilterContext filterContext) {
                return filterContext.<ListGroupsResponseData> sendRequest(ApiKeys.LIST_GROUPS.latestVersion(), new ListGroupsRequestData())
                        .thenApply(r -> {
                            if (r.errorCode() != Errors.NONE.code()) {
                                throw new RuntimeException("Async request unexpected failed (errorCode: %d)".formatted(r.errorCode()));
                            }
                            return r;
                        });
            }
        };

    }

    public static final int FILTER_NAME_TAG = 500;
    private final String name;
    private final Set<ApiKeys> keysToMark;
    private final Set<Direction> direction;
    private final ForwardingStyle forwardingStyle;

    public RequestResponseMarkingFilter(RequestResponseMarkingFilterConfig config) {
        name = config.name;
        keysToMark = config.keysToMark;
        direction = config.direction;
        forwardingStyle = config.forwardingStyle;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        if (!(direction.contains(Direction.REQUEST) && keysToMark.contains(apiKey))) {
            return filterContext.forwardRequest(header, body);
        }

        return forwardingStyle.apply(filterContext, body)
                .thenApply(request -> applyTaggedField(request, Direction.REQUEST, name))
                .thenCompose(taggedRequest -> filterContext.forwardRequest(header, taggedRequest));
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        if (!(direction.contains(Direction.RESPONSE) && keysToMark.contains(apiKey))) {
            return filterContext.forwardResponse(header, body);
        }

        return forwardingStyle.apply(filterContext, body)
                .thenApply(request -> applyTaggedField(request, Direction.RESPONSE, name))
                .thenCompose(taggedRequest -> filterContext.forwardResponse(header, taggedRequest));
    }

    private ApiMessage applyTaggedField(ApiMessage body, Direction direction, String name) {
        body.unknownTaggedFields().add(createTaggedField(direction.toString().toLowerCase(Locale.ROOT), name));
        return body;
    }

    private RawTaggedField createTaggedField(String type, String name) {
        return new RawTaggedField(FILTER_NAME_TAG, (this.getClass().getSimpleName() + "-" + name + "-" + type).getBytes(UTF_8));
    }

    public static class RequestResponseMarkingFilterConfig extends BaseConfig {
        private final String name;

        private final Set<ApiKeys> keysToMark;

        /**
         * Direction(s) to which the filter is applied
         */
        private final Set<Direction> direction;

        /*
         * If true, forward will occur after an asynchronous request is made and a response received from the broker.
         */
        private final ForwardingStyle forwardingStyle;

        @JsonCreator
        public RequestResponseMarkingFilterConfig(@JsonProperty(value = "name", required = true) String name,
                                                  @JsonProperty(value = "keysToMark", required = true) Set<ApiKeys> keysToMark,
                                                  @JsonProperty(value = "direction") Set<Direction> direction,
                                                  @JsonProperty(value = "forwardingStyle") ForwardingStyle forwardingStyle) {
            this.name = name;
            this.keysToMark = keysToMark;
            this.direction = direction == null || direction.isEmpty() ? Set.of(Direction.RESPONSE, Direction.REQUEST) : direction;
            this.forwardingStyle = forwardingStyle == null ? ForwardingStyle.SYNCHRONOUS : forwardingStyle;
        }
    }

}
