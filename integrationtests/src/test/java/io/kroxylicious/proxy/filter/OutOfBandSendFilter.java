/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.BaseConfig;

import static io.kroxylicious.UnknownTaggedFields.unknownTaggedFieldsToStrings;

/**
 * Designed to work in tandem with another Filter like {@link RequestResponseMarkingFilter}. This Filter targets
 * DescribeClusterRequest and sends an out-of-band request instead. Then, when it gets a response via the sendRequest
 * CompletionStage, it collects the Unknown Tagged Fields for a specified tag and forwards them as an error message in
 * a DescribeClusterResponse. This exposes what unknown tagged fields were added to the out-of-band response by other
 * filters like {@link RequestResponseMarkingFilter}.
 */
public class OutOfBandSendFilter implements DescribeClusterRequestFilter {

    private final OutOfBandSendFilterConfig config;

    public OutOfBandSendFilter(OutOfBandSendFilterConfig config) {
        this.config = config;
    }

    public static class OutOfBandSendFilterConfig extends BaseConfig {
        private final ApiKeys apiKeyToSend;
        private final int tagIdToCollect;

        @JsonCreator
        public OutOfBandSendFilterConfig(@JsonProperty(value = "apiKeyToSend", required = true) ApiKeys apiKeyToSend,
                                         @JsonProperty(value = "tagToCollect", required = true) int tagIdToCollect) {
            this.apiKeyToSend = apiKeyToSend;
            this.tagIdToCollect = tagIdToCollect;
        }

    }

    @Override
    public void onDescribeClusterRequest(short apiVersion, RequestHeaderData header, DescribeClusterRequestData request, KrpcFilterContext context) {
        ApiKeys apiKeyToSend = config.apiKeyToSend;
        ApiMessage message = createApiMessage(apiKeyToSend);
        context.sendRequest(apiKeyToSend.latestVersion(), message).thenAccept(apiMessage -> {
            String values = unknownTaggedFieldsToStrings(apiMessage, config.tagIdToCollect).collect(Collectors.joining(","));
            // still need to think about this, we already "closed" this context by discarding. maybe we need another context here
            // particular to the out-of-band response.
            context.forwardResponse(
                    new DescribeClusterResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                            .setErrorMessage("filterNameTaggedFieldsFromOutOfBandResponse: " + values));
        });
        // discard the request to let event loop process further requests
        context.discard();
    }

    private static ApiMessage createApiMessage(ApiKeys apiKeyToSend) {
        ApiMessage message;
        if (Objects.requireNonNull(apiKeyToSend) == ApiKeys.CREATE_TOPICS) {
            message = new CreateTopicsRequestData();
        }
        else {
            throw new IllegalArgumentException("apiKey " + apiKeyToSend + " not supported yet");
        }
        return message;
    }
}
