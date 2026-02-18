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
        return authorizationFilter.authorization(context, List.of(writeTxnAction)).thenCompose(authorizeResult -> {
            // only authorizing a single action, so can check if denied list is empty
            if (!authorizeResult.denied().isEmpty()) {
                AddOffsetsToTxnResponseData responseData = new AddOffsetsToTxnResponseData();
                responseData.setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
                return context.requestFilterResultBuilder().shortCircuitResponse(responseData).completed();
            }
            else {
                return context.forwardRequest(header, request);
            }
        });
    }
}
