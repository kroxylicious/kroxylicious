/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeGroupsResponse;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class DescribeGroupsEnforcement extends ApiEnforcement<DescribeGroupsRequestData, DescribeGroupsResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 6;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, DescribeGroupsRequestData request, FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        boolean isIncludeAuthorizedOps = request.includeAuthorizedOperations();
        List<GroupResource> actionsToAuthorize = actionsToAuthorize(isIncludeAuthorizedOps);
        List<Action> actions = request.groups().stream().flatMap(group -> actionsToAuthorize.stream().map(groupResource -> new Action(groupResource, group))).toList();
        return authorizationFilter.authorization(context, actions).thenCompose(authorizeResult -> {
            if (authorizeResult.allowed().isEmpty()) {
                return context.requestFilterResultBuilder().errorResponse(header, request, Errors.GROUP_AUTHORIZATION_FAILED.exception()).completed();
            }
            // we can only short-circuit if we are not including the proxy authorized operations
            else if (authorizeResult.denied().isEmpty() && !isIncludeAuthorizedOps) {
                return context.forwardRequest(header, request);
            }
            else {
                Map<Decision, List<String>> partitionedGroups = authorizeResult.partition(request.groups(), GroupResource.DESCRIBE, Function.identity());
                List<String> deniedGroups = partitionedGroups.get(Decision.DENY);
                request.groups().removeAll(deniedGroups);
                authorizationFilter.pushInflightState(header, (DescribeGroupsResponseData responseData) -> {
                    if (isIncludeAuthorizedOps) {
                        responseData.groups().forEach(describedGroup -> {
                            describedGroup.setAuthorizedOperations(
                                    AuthorizedOps.groupAuthorizedOps(authorizeResult, describedGroup.authorizedOperations(), describedGroup.groupId()));
                        });
                    }
                    for (String deniedGroup : deniedGroups) {
                        responseData.groups().add(groupAuthzFailureResult(deniedGroup));
                    }
                    return responseData;
                });
                return context.forwardRequest(header, request);
            }
        });
    }

    private static List<GroupResource> actionsToAuthorize(boolean isIncludeAuthorizedOps) {
        if (isIncludeAuthorizedOps) {
            // we need to know which operations are authorized to include them in the authorized ops
            return List.of(GroupResource.values());
        }
        else {
            // we only need to know if the actor can DESCRIBE
            return List.of(GroupResource.DESCRIBE);
        }
    }

    private static DescribedGroup groupAuthzFailureResult(String deniedGroup) {
        return new DescribedGroup().setGroupId(deniedGroup).setGroupState(DescribeGroupsResponse.UNKNOWN_STATE)
                .setProtocolType(DescribeGroupsResponse.UNKNOWN_PROTOCOL_TYPE)
                .setProtocolData(DescribeGroupsResponse.UNKNOWN_PROTOCOL)
                .setMembers(List.of())
                .setAuthorizedOperations(Integer.MIN_VALUE)
                .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code());
    }
}
