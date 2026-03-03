/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class FindCoordinatorEnforcement extends ApiEnforcement<FindCoordinatorRequestData, FindCoordinatorResponseData> {

    public static final int MIN_API_VERSION_USING_BATCHING = 4;
    public static final int MIN_API_VERSION_WITH_KEY = 1;

    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 6;
    }

    public static boolean usesBatching(RequestHeaderData header) {
        return header.requestApiVersion() >= MIN_API_VERSION_USING_BATCHING;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   FindCoordinatorRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        if (request.keyType() != FindCoordinatorRequest.CoordinatorType.TRANSACTION.id()) {
            return context.forwardRequest(header, request);
        }
        List<String> keys;
        if (usesBatching(header)) {
            keys = request.coordinatorKeys();
        }
        else {
            keys = List.of(request.key());
        }

        var actions = keys.stream()
                .map(key -> new Action(TransactionalIdResource.DESCRIBE, key))
                .toList();
        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    var decisions = authorization.partition(keys,
                            TransactionalIdResource.DESCRIBE,
                            Function.identity());
                    var allowedKeys = decisions.get(Decision.ALLOW);
                    var deniedKeys = decisions.get(Decision.DENY);
                    if (allowedKeys.isEmpty()) {
                        // Shortcircuit if there are no allowed topics
                        return context.requestFilterResultBuilder()
                                .shortCircuitResponse(FindCoordinatorEnforcement
                                        .errorResponse(deniedKeys, usesBatching(header)))
                                .completed();
                    }
                    else if (deniedKeys.isEmpty()) {
                        // Just forward if there are no denied topics
                        return context.forwardRequest(header, request);
                    }
                    else {
                        // Note: non-batched => singleton keys => one of the above branches must have been taken
                        // so in this branch we can assume a batched request
                        request.setCoordinatorKeys(allowedKeys);

                        var errorCoordinators = FindCoordinatorEnforcement
                                .errorResponse(deniedKeys, true)
                                .coordinators();

                        authorizationFilter.pushInflightState(header, (FindCoordinatorResponseData response) -> {
                            response.coordinators().addAll(errorCoordinators);
                            return response;
                        });
                        return context.forwardRequest(header, request);
                    }
                });
    }

    private static FindCoordinatorResponseData errorResponse(List<String> keys, boolean usesBatching) {
        if (usesBatching) {
            var list = keys.stream()
                    .map(key -> new FindCoordinatorResponseData.Coordinator()
                            .setKey(key)
                            .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code())
                            // Kafka does not use the default error message for batched requests (for some reason)
                            .setPort(-1)
                            .setHost("")
                            .setNodeId(-1))
                    .toList();
            return new FindCoordinatorResponseData().setCoordinators(list);
        }
        else {
            return new FindCoordinatorResponseData()
                    .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code())
                    .setErrorMessage(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.message())
                    .setPort(-1)
                    .setHost("")
                    .setNodeId(-1);
        }
    }
}
