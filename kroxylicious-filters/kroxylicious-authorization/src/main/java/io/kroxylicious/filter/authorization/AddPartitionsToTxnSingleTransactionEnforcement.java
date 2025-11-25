/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

class AddPartitionsToTxnSingleTransactionEnforcement extends ApiEnforcement<AddPartitionsToTxnRequestData, AddPartitionsToTxnResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 3;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   AddPartitionsToTxnRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<Action> actions = request.v3AndBelowTopics().stream().map(s -> new Action(TopicResource.WRITE, s.name())).toList();
        CompletionStage<AuthorizeResult> authorization = authorizationFilter.authorization(context, actions);
        return authorization.thenCompose(result -> {
            if (result.denied().isEmpty()) {
                return context.forwardRequest(header, request);
            }
            else {
                // add partitions succeeds or fails atomically for a transaction, allowed topics fail with OPERATION_NOT_ATTEMPTED
                Set<String> deniedTopics = result.denied().stream().map(Action::resourceName).collect(Collectors.toSet());
                AddPartitionsToTxnResponseData response = new AddPartitionsToTxnResponseData();
                List<AddPartitionsToTxnTopicResult> topicResults = request.v3AndBelowTopics().stream().map(topic -> {
                    AddPartitionsToTxnTopicResult txnTopicResult = new AddPartitionsToTxnTopicResult();
                    txnTopicResult.setName(topic.name());
                    topic.partitions().forEach(partition -> {
                        AddPartitionsToTxnPartitionResult partitionResult = new AddPartitionsToTxnPartitionResult();
                        partitionResult.setPartitionIndex(partition);
                        Errors error = deniedTopics.contains(topic.name()) ? Errors.TOPIC_AUTHORIZATION_FAILED : Errors.OPERATION_NOT_ATTEMPTED;
                        partitionResult.setPartitionErrorCode(error.code());
                        txnTopicResult.resultsByPartition().add(partitionResult);
                    });
                    return txnTopicResult;
                }).toList();
                // results are reversed in the real implementation for some reason
                for (int i = topicResults.size() - 1; i >= 0; i--) {
                    response.resultsByTopicV3AndBelow().mustAdd(topicResults.get(i));
                }

                return context.requestFilterResultBuilder().shortCircuitResponse(response).completed();
            }
        });
    }
}
