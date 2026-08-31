/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.JoinGroupRequestData;
import io.kroxylicious.kafka.common.message.JoinGroupResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * Enforces authorization of the JoinGroup API, requiring {@link GroupResource#READ}
 * on the consumer group.
 */
public class JoinGroupEnforcement extends ApiEnforcement<JoinGroupRequestData, JoinGroupResponseData> {

    /**
     * Creates the enforcement.
     */
    public JoinGroupEnforcement() {
        // Intentionally empty
    }

    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 9;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   JoinGroupRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        Action readGroup = new Action(GroupResource.READ, request.groupId());
        return authorizationFilter.authorization(context, List.of(readGroup)).thenCompose(authorizeResult -> {
            if (authorizeResult.denied().contains(readGroup)) {
                return context.requestFilterResultBuilder().errorResponse(header, request, Errors.GROUP_AUTHORIZATION_FAILED).completed();
            }
            else {
                return context.forwardRequest(header, request);
            }
        });
    }
}
