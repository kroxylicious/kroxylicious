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
        // While v6 of the RPC is defined, its use is gated on the transaction.version
        // feature having transaction.version >= 3.
        // Kafka 4.2 has `transaction.version -> SupportedVersionRange[min_version:0, max_version:2]`
        // Thus while API version 6 is defined it's not possible to spin up a broker what actually supports it,
        // and therefore it's impossible to test.
        // See https://cwiki.apache.org/confluence/display/KAFKA/KIP-939:+Support+Participation+in+2PC#KIP939:SupportParticipationin2PC-Compatibility,Deprecation,andMigrationPlan
        return MIN_VERSION_SUPPORTING_2PC - 1;
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
                        else {
                            // Shortcircuit if there are no allowed actions
                            return context.requestFilterResultBuilder().shortCircuitResponse(
                                    errorResponse()).completed();
                        }
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
