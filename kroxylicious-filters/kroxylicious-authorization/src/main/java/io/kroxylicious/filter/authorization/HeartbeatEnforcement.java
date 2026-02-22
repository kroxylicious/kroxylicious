/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class HeartbeatEnforcement extends ApiEnforcement<HeartbeatRequestData, HeartbeatResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 4;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, HeartbeatRequestData request, FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        Action readGroup = new Action(GroupResource.READ, request.groupId());
        return authorizationFilter.authorization(context, List.of(readGroup)).thenCompose(authorizeResult -> {
            if (authorizeResult.denied().contains(readGroup)) {
                return context.requestFilterResultBuilder().errorResponse(header, request, Errors.GROUP_AUTHORIZATION_FAILED.exception()).completed();
            }
            else {
                return context.forwardRequest(header, request);
            }
        });
    }
}
