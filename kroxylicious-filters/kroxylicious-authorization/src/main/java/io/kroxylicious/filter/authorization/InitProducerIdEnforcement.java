/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import edu.umd.cs.findbugs.annotations.Nullable;

public class InitProducerIdEnforcement extends ApiEnforcement<InitProducerIdRequestData, InitProducerIdResponseData> {

    public static final short MIN_VERSION_SUPPORTING_2PC = 6;

    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 5; // last version before support for 2PC
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   InitProducerIdRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {

        @Nullable
        var transactionalId = request.transactionalId();
        List<Action> actions;
        if (isTransactional(transactionalId)) {
            Action writeTransactionalId = new Action(TransactionalIdResource.WRITE, transactionalId);
            if (request.enable2Pc()) {
                actions = List.of(writeTransactionalId,
                        new Action(TransactionalIdResource.TWO_PHASE_COMMIT, transactionalId));
            }
            else {
                actions = List.of(writeTransactionalId);
            }
        }
        else {
            // TODO actions = List.of(new Action(ClusterResource.IDEMPOTENT_WRITE, ""));
            actions = new ArrayList<>();
        }
        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    if (isTransactional(transactionalId)) {
                        if (authorization.denied().isEmpty()) {
                            // Just forward if there are no denied actions
                            return context.forwardRequest(header, request);
                        }
                        else if (authorization.allowed().isEmpty()) {
                            // Shortcircuit if there are no allowed actions
                            return context.requestFilterResultBuilder().shortCircuitResponse(
                                    errorResponse()).completed();
                        }
                        // Note: Because this request does not support batching there should not be any other cases.
                        throw new IllegalStateException();
                    }
                    else {
                        return context.forwardRequest(header, request);
                    }
                });
    }

    private static boolean isTransactional(@Nullable String transactionalId) {
        return transactionalId != null;
    }

    private InitProducerIdResponseData errorResponse() {
        return new InitProducerIdResponseData()
                .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code())
                .setProducerEpoch((short) -1);
    }

}
