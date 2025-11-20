/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetFetchResponse;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class OffsetFetchNonBatchingEnforcement extends ApiEnforcement<OffsetFetchRequestData, OffsetFetchResponseData> {
    // lowest version supported by proxy
    @Override
    short minSupportedVersion() {
        return 1;
    }

    @Override
    short maxSupportedVersion() {
        return 7;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   OffsetFetchRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<Action> actions = request.topics().stream()
                .map(ofrd -> new Action(TopicResource.DESCRIBE, ofrd.name()))
                .toList();
        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    var decisions = authorization.partition(request.topics(), TopicResource.DESCRIBE, OffsetFetchRequestData.OffsetFetchRequestTopic::name);
                    if (decisions.get(Decision.DENY).isEmpty()) {
                        return context.forwardRequest(header, request);
                    }
                    else if (decisions.get(Decision.ALLOW).isEmpty()) {
                        return denyAll(context, decisions);
                    }
                    else {
                        return removeDeniedTopicsForwardAndMergeResponse(header, request, context, authorizationFilter, authorization, decisions);
                    }
                });
    }

    private static CompletionStage<RequestFilterResult> removeDeniedTopicsForwardAndMergeResponse(RequestHeaderData header, OffsetFetchRequestData request,
                                                                                                  FilterContext context, AuthorizationFilter authorizationFilter,
                                                                                                  AuthorizeResult authorization,
                                                                                                  Map<Decision, List<OffsetFetchRequestData.OffsetFetchRequestTopic>> t1) {
        var allowed = request.topics().stream().filter(t -> authorization.decision(TopicResource.DESCRIBE, t.name()) == Decision.ALLOW).toList();
        var offsetFetchResponseTopics = createDeniedTopicResponses(t1.get(Decision.DENY));
        authorizationFilter.pushInflightState(header, (OffsetFetchResponseData response) -> {
            response.topics().addAll(offsetFetchResponseTopics);
            return response;
        });
        request.setTopics(allowed);
        return context.forwardRequest(header, request);
    }

    private static CompletionStage<RequestFilterResult> denyAll(FilterContext context,
                                                                Map<Decision, List<OffsetFetchRequestData.OffsetFetchRequestTopic>> t1) {
        var offsetFetchResponseTopics = createDeniedTopicResponses(t1.get(Decision.DENY));
        OffsetFetchResponseData response = new OffsetFetchResponseData();
        response.setTopics(offsetFetchResponseTopics);
        return context.requestFilterResultBuilder()
                .shortCircuitResponse(response)
                .completed();
    }

    private static List<OffsetFetchResponseData.OffsetFetchResponseTopic> createDeniedTopicResponses(
                                                                                                     List<OffsetFetchRequestData.OffsetFetchRequestTopic> offsetFetchRequestTopics) {
        return offsetFetchRequestTopics.stream().map(x -> new OffsetFetchResponseData.OffsetFetchResponseTopic()
                .setName(x.name())
                .setPartitions(x.partitionIndexes().stream()
                        .map(OffsetFetchNonBatchingEnforcement::unauthorizedPartition)
                        .toList()))
                .toList();
    }

    private static OffsetFetchResponseData.OffsetFetchResponsePartition unauthorizedPartition(int partitionIndex) {
        return new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(partitionIndex)
                .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                .setMetadata(OffsetFetchResponse.NO_METADATA)
                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }
}
