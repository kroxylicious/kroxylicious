/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

public class CreateTopicRejectFilter implements CreateTopicsRequestFilter {

    public static final String ERROR_MESSAGE = "rejecting all topics";

    @Override
    public CompletionStage<RequestFilterResult> onCreateTopicsRequest(short apiVersion, RequestHeaderData header, CreateTopicsRequestData request,
                                                                      KrpcFilterContext context) {
        CreateTopicsResponseData response = new CreateTopicsResponseData();
        CreateTopicsResponseData.CreatableTopicResultCollection topics = new CreateTopicsResponseData.CreatableTopicResultCollection();
        allocateByteBufToTestKroxyliciousReleasesIt(context);
        request.topics().forEach(creatableTopic -> {
            CreateTopicsResponseData.CreatableTopicResult result = new CreateTopicsResponseData.CreatableTopicResult();
            result.setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code()).setErrorMessage(ERROR_MESSAGE);
            result.setName(creatableTopic.name());
            topics.add(result);
        });
        response.setTopics(topics);
        return CompletableFuture.completedStage(context.requestFilterResultBuilder().asRequestShortCircuitResponse().withMessage(response).build());
    }

    private static void allocateByteBufToTestKroxyliciousReleasesIt(KrpcFilterContext context) {
        context.createByteBufferOutputStream(4000);
    }
}
