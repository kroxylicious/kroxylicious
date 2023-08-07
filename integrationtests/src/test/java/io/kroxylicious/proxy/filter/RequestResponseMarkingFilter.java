/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletionStage;

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
 * specified set of ApiKeys. The tags have values:
 * <ul>
 *     <li>Requests: RequestResponseMarkingFilter-${config.name}-request</li>
 *     <li>Responses: RequestResponseMarkingFilter-${config.name}-response</li>
 * </ul>
 */
public class RequestResponseMarkingFilter implements RequestFilter, ResponseFilter {

    private final boolean forwardAsync;

    public enum Direction {
        REQUEST,
        RESPONSE;
    }

    public static final int FILTER_NAME_TAG = 500;
    private final String name;
    private final Set<ApiKeys> keysToMark;
    private final Set<Direction> direction;

    public RequestResponseMarkingFilter(RequestResponseMarkingFilterConfig config) {
        name = config.name;
        keysToMark = config.keysToMark;
        direction = config.direction;
        forwardAsync = config.forwardAsync;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        if (!direction.contains(Direction.REQUEST)) {
            return filterContext.forwardRequest(header, body);
        }
        if (forwardAsync) {
            return sendAsyncRequestAndCheckForResponseErrors(filterContext).thenCompose(r -> {
                applyTaggedFieldIfNecessary(apiKey, body, Direction.REQUEST);
                return filterContext.forwardRequest(header, body);
            });
        }
        else {
            applyTaggedFieldIfNecessary(apiKey, body, Direction.REQUEST);
            return filterContext.forwardRequest(header, body);
        }
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        if (!direction.contains(Direction.RESPONSE)) {
            return filterContext.forwardResponse(header, body);
        }
        if (forwardAsync) {
            return sendAsyncRequestAndCheckForResponseErrors(filterContext).thenCompose(r -> {
                applyTaggedFieldIfNecessary(apiKey, body, Direction.RESPONSE);
                return filterContext.forwardResponse(header, body);
            });
        }
        else {
            applyTaggedFieldIfNecessary(apiKey, body, Direction.RESPONSE);
            return filterContext.forwardResponse(header, body);
        }
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

    private void applyTaggedFieldIfNecessary(ApiKeys apiKey, ApiMessage body, Direction direction) {
        if (keysToMark.contains(apiKey)) {
            body.unknownTaggedFields().add(createTaggedField(direction.toString().toLowerCase(Locale.ROOT)));
        }
    }

    private RawTaggedField createTaggedField(String type) {
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
        private final boolean forwardAsync;

        @JsonCreator
        public RequestResponseMarkingFilterConfig(@JsonProperty(value = "name", required = true) String name,
                                                  @JsonProperty(value = "keysToMark", required = true) Set<ApiKeys> keysToMark,
                                                  @JsonProperty(value = "direction") Set<Direction> direction,
                                                  @JsonProperty(value = "forwardAsync") boolean forwardAsync) {
            this.name = name;
            this.direction = direction == null || direction.isEmpty() ? Set.of(Direction.RESPONSE, Direction.REQUEST) : direction;
            this.keysToMark = keysToMark;
            this.forwardAsync = forwardAsync;
        }
    }

}
