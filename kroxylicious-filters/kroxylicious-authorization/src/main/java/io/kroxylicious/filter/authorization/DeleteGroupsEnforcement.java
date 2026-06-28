/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class DeleteGroupsEnforcement extends ApiEnforcement<DeleteGroupsRequestData, DeleteGroupsResponseData> {
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
                return context.requestFilterResultBuilder().errorResponse(header, request, Errors.GROUP_AUTHORIZATION_FAILED.exception()).completed();
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
