/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData.TopicData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData.TopicDataCollection;
import org.apache.kafka.common.message.DescribeTransactionsResponseData.TransactionState;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

public class DescribeTransactionsEnforcement extends ApiEnforcement<DescribeTransactionsRequestData, DescribeTransactionsResponseData> {

    public static final TopicDataCollection EMPTY_TOPICS = new TopicDataCollection();

    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 0;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, DescribeTransactionsRequestData request, FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        Stream<Action> transactionalIds = request.transactionalIds().stream().map(transactionalId -> new Action(TransactionalIdResource.DESCRIBE, transactionalId));
        return authorizationFilter.authorization(context, transactionalIds.toList()).thenCompose(authorizeResult -> {
            List<TransactionState> unauthorizedResponseTransactions = new ArrayList<>();
            List<String> deniedTransactionIds = authorizeResult.denied(TransactionalIdResource.DESCRIBE);
            for (String transactionalId : deniedTransactionIds) {
                TransactionState transactionState = new TransactionState().setTransactionalId(transactionalId)
                        .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
                unauthorizedResponseTransactions.add(transactionState);
            }

            if (authorizeResult.allowed().isEmpty()) {
                DescribeTransactionsResponseData message = new DescribeTransactionsResponseData();
                message.setTransactionStates(unauthorizedResponseTransactions);
                return context.requestFilterResultBuilder().shortCircuitResponse(message).completed();
            }
            else {
                if (!authorizeResult.denied().isEmpty()) {
                    request.transactionalIds().removeAll(deniedTransactionIds);
                    authorizationFilter.pushInflightState(header, (DescribeTransactionsResponseData responseData) -> {
                        responseData.transactionStates().addAll(unauthorizedResponseTransactions);
                        return responseData;
                    });
                }
                return context.forwardRequest(header, request);
            }
        });
    }

    @Override
    CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header, DescribeTransactionsResponseData response, FilterContext context,
                                                     AuthorizationFilter authorizationFilter) {
        List<TransactionState> transactionStates = Objects.requireNonNullElse(response.transactionStates(), List.of());
        List<Action> describeTopics = transactionStates.stream().flatMap(transactionState -> {
            TopicDataCollection topics = Objects.requireNonNullElse(transactionState.topics(), EMPTY_TOPICS);
            return topics.stream().map(TopicData::topic);
        }).distinct().map(topic -> new Action(TopicResource.DESCRIBE, topic)).toList();
        return authorizationFilter.authorization(context, describeTopics).thenCompose(authorizeResult -> {
            HashSet<String> deniedTopics = new HashSet<>(authorizeResult.denied(TopicResource.DESCRIBE));
            transactionStates.forEach(transactionState -> {
                TopicDataCollection topics = Objects.requireNonNullElse(transactionState.topics(), EMPTY_TOPICS);
                topics.removeIf(next -> deniedTopics.contains(next.topic()));
            });
            return super.onResponse(header, response, context, authorizationFilter);
        });
    }

}
