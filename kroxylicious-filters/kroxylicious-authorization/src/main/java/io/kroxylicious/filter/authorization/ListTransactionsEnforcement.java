/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.ListTransactionsResponseData.TransactionState;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

class ListTransactionsEnforcement extends ApiEnforcement<ListTransactionsRequestData, ListTransactionsResponseData> {

    short minSupportedVersion() {
        return 0;
    }

    short maxSupportedVersion() {
        return 2;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, ListTransactionsRequestData request, FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        return context.forwardRequest(header, request);
    }

    @Override
    CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header, ListTransactionsResponseData response, FilterContext context,
                                                     AuthorizationFilter authorizationFilter) {
        List<Action> actions = Objects.requireNonNull(response.transactionStates()).stream().map(TransactionState::transactionalId)
                .map(txnlId -> new Action(TransactionalIdResource.DESCRIBE, txnlId))
                .toList();
        return authorizationFilter.authorization(context, actions).thenCompose(authorizeResult -> {
            List<String> deniedTxnlIds = authorizeResult.denied(TransactionalIdResource.DESCRIBE);
            response.transactionStates().removeIf(transactionState -> deniedTxnlIds.contains(transactionState.transactionalId()));
            return super.onResponse(header, response, context, authorizationFilter);
        });
    }
}
