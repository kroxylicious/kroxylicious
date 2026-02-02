/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

class TxnOffsetCommitEnforcement extends ApiEnforcement<TxnOffsetCommitRequestData, TxnOffsetCommitResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 5;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   TxnOffsetCommitRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        Stream<Action> topicActions = request.topics().stream().map(TxnOffsetCommitRequestTopic::name).map(s -> new Action(TopicResource.READ, s));
        Stream<Action> transactionAction = Stream.of(new Action(TransactionalIdResource.WRITE, request.transactionalId()));
        List<Action> actions = Stream.concat(topicActions, transactionAction).toList();
        CompletionStage<AuthorizeResult> authorization = authorizationFilter.authorization(context, actions);
        return authorization.thenCompose(result -> {
            Decision transactionalResult = result.decision(TransactionalIdResource.WRITE, request.transactionalId());
            if (transactionalResult == Decision.DENY) {
                // short circuit, transactional id write denied
                TxnOffsetCommitResponseData message = new TxnOffsetCommitResponseData();
                for (TxnOffsetCommitRequestTopic topic : request.topics()) {
                    message.topics().add(errorResponseTopic(topic, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED));
                }
                return context.requestFilterResultBuilder().shortCircuitResponse(message).completed();
            }
            Map<Decision, List<TxnOffsetCommitRequestTopic>> partitioned = result.partition(request.topics(), TopicResource.READ,
                    TxnOffsetCommitRequestTopic::name);
            if (partitioned.get(Decision.DENY).isEmpty()) {
                return context.forwardRequest(header, request);
            }
            else {
                List<TxnOffsetCommitRequestTopic> toRemove = partitioned.get(Decision.DENY);
                List<TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic> removed = toRemove.stream().map(
                        requestTopic -> errorResponseTopic(requestTopic, Errors.TOPIC_AUTHORIZATION_FAILED)).toList();
                if (partitioned.get(Decision.ALLOW).isEmpty()) {
                    // short circuit, no topics in request allowed
                    TxnOffsetCommitResponseData message = new TxnOffsetCommitResponseData();
                    message.topics().addAll(removed);
                    return context.requestFilterResultBuilder().shortCircuitResponse(message).completed();
                }
                else {
                    request.topics().removeAll(toRemove);
                    authorizationFilter.pushInflightState(header, (TxnOffsetCommitResponseData response) -> {
                        // this RPC appears to have errors first in the response
                        response.topics().addAll(0, removed);
                        return response;
                    });
                    return context.forwardRequest(header, request);
                }
            }
        });
    }

    private static TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic errorResponseTopic(TxnOffsetCommitRequestTopic requestTopic, Errors error) {
        TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic responseTopic = new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic();
        responseTopic.setName(requestTopic.name());
        List<TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition> partitionErrors = requestTopic.partitions().stream()
                .map(requestPartition -> new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition().setPartitionIndex(
                        requestPartition.partitionIndex())
                        .setErrorCode(error.code()))
                .toList();
        responseTopic.setPartitions(partitionErrors);
        return responseTopic;
    }
}
