/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.DeleteGroupsRequestData;
import io.kroxylicious.kafka.common.message.DeleteGroupsResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * Enforces authorization of the DeleteGroups API, requiring {@link GroupResource#DELETE}
 * on each consumer group named in the request.
 */
public class DeleteGroupsEnforcement extends ApiEnforcement<DeleteGroupsRequestData, DeleteGroupsResponseData> {

    /**
     * Creates the enforcement.
     */
    public DeleteGroupsEnforcement() {
        // Intentionally empty
    }

    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 2;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   DeleteGroupsRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<Action> groupDeleteActions = request.groupsNames().stream().map(group -> new Action(GroupResource.DELETE, group)).toList();
        return authorizationFilter.authorization(context, groupDeleteActions).thenCompose(authorizeResult -> {
            if (authorizeResult.denied().isEmpty()) {
                return context.forwardRequest(header, request);
            }
            else if (authorizeResult.allowed().isEmpty()) {
                return context.requestFilterResultBuilder().errorResponse(header, request, Errors.GROUP_AUTHORIZATION_FAILED).completed();
            }
            else {
                Map<Decision, List<String>> partitioned = authorizeResult.partition(request.groupsNames(), GroupResource.DELETE, s -> s);
                List<String> deniedGroups = partitioned.get(Decision.DENY);
                request.groupsNames().removeAll(deniedGroups);
                authorizationFilter.pushInflightState(header, (DeleteGroupsResponseData responseData) -> {
                    for (String deniedGroup : deniedGroups) {
                        DeleteGroupsResponseData.DeletableGroupResult result = new DeleteGroupsResponseData.DeletableGroupResult();
                        result.setGroupId(deniedGroup);
                        result.setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code());
                        responseData.results().add(result);
                    }
                    return responseData;
                });
                return context.forwardRequest(header, request);
            }
        });
    }
}
