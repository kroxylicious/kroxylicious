/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.BaseConfig;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A Filter that adds an UnknownTaggedField to all request/response messages for some
 * specified set of ApiKeys. The tags have values:
 * <ul>
 *     <li>Requests: RequestResponseMarkingFilter-${config.name}-request</li>
 *     <li>Responses: RequestResponseMarkingFilter-${config.name}-response</li>
 * </ul>
 */
public class RequestResponseMarkingFilter implements RequestFilter, ResponseFilter {

    public static final int FILTER_NAME_TAG = 500;
    private final String name;
    private final Set<ApiKeys> keysToMark;

    public RequestResponseMarkingFilter(RequestResponseMarkingFilterConfig config) {
        name = config.name;
        keysToMark = config.keysToMark;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        if (keysToMark.contains(apiKey)) {
            body.unknownTaggedFields().add(createTaggedField("request"));
        }
        return filterContext.forwardRequest(header, body);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        if (keysToMark.contains(apiKey)) {
            body.unknownTaggedFields().add(createTaggedField("response"));
        }
        return filterContext.forwardResponse(header, body);
    }

    private RawTaggedField createTaggedField(String type) {
        return new RawTaggedField(FILTER_NAME_TAG, (this.getClass().getSimpleName() + "-" + name + "-" + type).getBytes(UTF_8));
    }

    public static class RequestResponseMarkingFilterConfig extends BaseConfig {
        private final String name;

        private final Set<ApiKeys> keysToMark;

        @JsonCreator
        public RequestResponseMarkingFilterConfig(@JsonProperty(value = "name", required = true) String name,
                                                  @JsonProperty(value = "keysToMark", required = true) Set<ApiKeys> keysToMark) {
            this.name = name;
            this.keysToMark = keysToMark;
        }
    }

}
