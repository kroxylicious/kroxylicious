/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

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

import edu.umd.cs.findbugs.annotations.NonNull;

class AddPartitionsToTxnEnforcement extends ApiEnforcement<AddPartitionsToTxnRequestData, AddPartitionsToTxnResponseData> {

    // AddPartitionsToTxn is used client-to-broker only up to version 3
    // AddPartitionsToTxn version 4 and above are only allowed broker-to-broker
    public static final short MAX_CLIENT_TO_BROKER_VERSION = 3;

    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return MAX_CLIENT_TO_BROKER_VERSION;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   AddPartitionsToTxnRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {

        List<Action> actions = Stream.concat(
                Stream.of(new Action(TransactionalIdResource.WRITE, request.v3AndBelowTransactionalId())),
                request.v3AndBelowTopics().stream().map(s -> new Action(TopicResource.WRITE, s.name())))
                .toList();
        CompletionStage<AuthorizeResult> authorization = authorizationFilter.authorization(context, actions);
        return authorization.thenCompose(result -> {
            if (result.denied().isEmpty()) {
                return context.forwardRequest(header, request);
            }
            else {
                List<AddPartitionsToTxnTopicResult> topicResults;
                // the check for WRITE on TransactionalId takes precedence
                if (!result.denied(TransactionalIdResource.WRITE).isEmpty()) {
                    topicResults = request.v3AndBelowTopics().stream()
                            .map(topic -> partitionError(topic, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED))
                            .toList();
                }
                else {
                    // add partitions succeeds or fails atomically for a transaction, allowed topics fail with OPERATION_NOT_ATTEMPTED
                    Set<String> deniedTopics = new HashSet<>(result.denied(TopicResource.WRITE));
                    topicResults = request.v3AndBelowTopics().stream().map(topic -> {
                        Errors error = deniedTopics.contains(topic.name()) ? Errors.TOPIC_AUTHORIZATION_FAILED : Errors.OPERATION_NOT_ATTEMPTED;
                        return partitionError(topic, error);
                    }).toList();
                }
                AddPartitionsToTxnResponseData response = new AddPartitionsToTxnResponseData();
                // results are reversed in the real implementation for some reason
                for (int i = topicResults.size() - 1; i >= 0; i--) {
                    response.resultsByTopicV3AndBelow().mustAdd(topicResults.get(i));
                }

                return context.requestFilterResultBuilder().shortCircuitResponse(response).completed();
            }
        });
    }

    @NonNull
    private static AddPartitionsToTxnTopicResult partitionError(AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic topic,
                                                                Errors error) {
        AddPartitionsToTxnTopicResult txnTopicResult = new AddPartitionsToTxnTopicResult();
        txnTopicResult.setName(topic.name());
        topic.partitions().forEach(partition -> {
            AddPartitionsToTxnPartitionResult partitionResult = new AddPartitionsToTxnPartitionResult();
            partitionResult.setPartitionIndex(partition);
            partitionResult.setPartitionErrorCode(error.code());
            txnTopicResult.resultsByPartition().add(partitionResult);
        });
        return txnTopicResult;
    }
}
