/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class AddOffsetsToTxnEnforcement extends ApiEnforcement<AddOffsetsToTxnRequestData, AddOffsetsToTxnResponseData> {

    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 4;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, AddOffsetsToTxnRequestData request, FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        Action writeTxnAction = new Action(TransactionalIdResource.WRITE, request.transactionalId());
        Action groupReadAction = new Action(GroupResource.READ, request.groupId());
        return authorizationFilter.authorization(context, List.of(writeTxnAction, groupReadAction)).thenCompose(authorizeResult -> {
            // only authorizing a single action, so can check if denied list is empty
            if (!authorizeResult.denied().isEmpty()) {
                AddOffsetsToTxnResponseData responseData = new AddOffsetsToTxnResponseData();
                Errors error = getError(authorizeResult, writeTxnAction, groupReadAction);
                responseData.setErrorCode(error.code());
                return context.requestFilterResultBuilder().shortCircuitResponse(responseData).completed();
            }
            else {
                return context.forwardRequest(header, request);
            }
        });
    }

    private static Errors getError(AuthorizeResult authorizeResult, Action writeTxnAction, Action groupReadAction) {
        if (authorizeResult.denied().contains(writeTxnAction)) {
            return Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED;
        }
        else if (authorizeResult.denied().contains(groupReadAction)) {
            return Errors.GROUP_AUTHORIZATION_FAILED;
        }
        else {
            throw new IllegalStateException("unexpected action in denied set" + authorizeResult);
        }
    }
}
