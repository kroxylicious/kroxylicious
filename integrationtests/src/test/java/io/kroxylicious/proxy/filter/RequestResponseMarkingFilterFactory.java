/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.PluginConfigType;
import io.kroxylicious.proxy.plugin.Plugins;

import static java.nio.charset.StandardCharsets.UTF_8;

@PluginConfigType(RequestResponseMarkingFilterFactory.Config.class)
public class RequestResponseMarkingFilterFactory implements FilterFactory<RequestResponseMarkingFilterFactory.Config, RequestResponseMarkingFilterFactory.Config> {

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context,
                               Config configuration) {
        return new Filter(context, configuration);
    }

    public enum Direction {
        REQUEST,
        RESPONSE;
    }

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
    public static class Filter implements RequestFilter, ResponseFilter {

        public static final int FILTER_NAME_TAG = 500;
        private final FilterFactoryContext constructionContext;
        private final String name;
        private final Set<ApiKeys> keysToMark;
        private final Set<RequestResponseMarkingFilterFactory.Direction> direction;
        private final ForwardingStyle forwardingStyle;

        public Filter(FilterFactoryContext constructionContext, Config config) {
            this.constructionContext = constructionContext;
            name = config.name;
            keysToMark = config.keysToMark;
            direction = config.direction;
            forwardingStyle = config.forwardingStyle;
        }

        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, FilterContext context) {
            if (!(direction.contains(RequestResponseMarkingFilterFactory.Direction.REQUEST) && keysToMark.contains(apiKey))) {
                return context.forwardRequest(header, body);
            }

            return forwardingStyle.apply(new ForwardingContext(context, constructionContext, body))
                    .thenApply(request -> applyTaggedField(request, RequestResponseMarkingFilterFactory.Direction.REQUEST, name))
                    .thenCompose(taggedRequest -> context.forwardRequest(header, taggedRequest));
        }

        @Override
        public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {
            if (!(direction.contains(RequestResponseMarkingFilterFactory.Direction.RESPONSE) && keysToMark.contains(apiKey))) {
                return context.forwardResponse(header, response);
            }

            return forwardingStyle.apply(new ForwardingContext(context, constructionContext, response))
                    .thenApply(request -> applyTaggedField(request, RequestResponseMarkingFilterFactory.Direction.RESPONSE, name))
                    .thenCompose(taggedRequest -> context.forwardResponse(header, taggedRequest));
        }

        private ApiMessage applyTaggedField(ApiMessage body, RequestResponseMarkingFilterFactory.Direction direction, String name) {
            body.unknownTaggedFields().add(createTaggedField(direction.toString().toLowerCase(Locale.ROOT), name));
            return body;
        }

        private RawTaggedField createTaggedField(String type, String name) {
            return new RawTaggedField(FILTER_NAME_TAG,
                    (this.getClass().getEnclosingClass().getSimpleName() + "." + this.getClass().getSimpleName() + "-" + name + "-" + type).getBytes(UTF_8));
        }

    }

    public static class Config {
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
        public Config(@JsonProperty(value = "name", required = true) String name,
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
