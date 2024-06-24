/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.RequestHeaderData;

public class ConsumerGroupTopicMappingFilter implements
        OffsetCommitRequestFilter {

    @Override
    public CompletionStage<RequestFilterResult> onOffsetCommitRequest(short apiVersion, RequestHeaderData header, OffsetCommitRequestData request,
                                                                      FilterContext context) {

        var stages = new ArrayList<CompletionStage<OffsetCommitResponseData>>();

        for (var i = 0; i < request.topics().size(); i++) {
            var offsetCommitRequestTopic = request.topics().get(i).duplicate();
            var req = request.duplicate()
                    .setGroupId(request.groupId() + ":" + i)
                    .setTopics(Collections.singletonList(offsetCommitRequestTopic));
            stages.add(context.sendRequest(header, request));
        }

        return chainListOffsetsRequest(stages)
                .thenCompose(response -> context.requestFilterResultBuilder().shortCircuitResponse(response).completed());
    }

    private CompletionStage<OffsetCommitResponseData> chainListOffsetsRequest(ArrayList<CompletionStage<OffsetCommitResponseData>> stages) {
        // Start with an initial CompletionStage that has an empty Result
        CompletionStage<OffsetCommitResponseData> result = CompletableFuture.completedStage(new OffsetCommitResponseData());

        // Chain each CompletionStage
        for (CompletionStage<OffsetCommitResponseData> stage : stages) {
            result = result.thenCompose(prevResult -> stage.thenApply(currentResult -> {
                prevResult.topics().addAll(currentResult.topics());
                return prevResult;
            }));
        }

        return result;
    }

}