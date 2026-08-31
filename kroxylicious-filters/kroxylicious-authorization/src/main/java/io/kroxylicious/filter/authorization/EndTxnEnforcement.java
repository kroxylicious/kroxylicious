/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.EndTxnRequestData;
import io.kroxylicious.kafka.common.message.EndTxnResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * Enforces authorization of the EndTxn API, requiring {@link TransactionalIdResource#WRITE}
 * on the transactional id.
 */
public class EndTxnEnforcement extends ApiEnforcement<EndTxnRequestData, EndTxnResponseData> {

    /**
     * Creates the enforcement.
     */
    public EndTxnEnforcement() {
        // Intentionally empty
    }

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
                                                   EndTxnRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        String transactionalId = Objects.requireNonNull(request.transactionalId(), "transactionId was null");
        Action writeTransaction = new Action(TransactionalIdResource.WRITE, transactionalId);
        return authorizationFilter.authorization(context, List.of(writeTransaction)).thenCompose(authorizeResult -> {
            if (authorizeResult.denied().contains(writeTransaction)) {
                EndTxnResponseData errorResponse = new EndTxnResponseData();
                errorResponse.setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code());
                return context.requestFilterResultBuilder().shortCircuitResponse(errorResponse).completed();
            }
            else {
                return context.forwardRequest(header, request);
            }
        });
    }
}
