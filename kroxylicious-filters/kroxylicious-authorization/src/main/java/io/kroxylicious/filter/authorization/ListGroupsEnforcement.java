/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData.ListedGroup;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import static io.kroxylicious.filter.authorization.GroupResource.DESCRIBE;

public class ListGroupsEnforcement extends ApiEnforcement<ListGroupsRequestData, ListGroupsResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 5;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, ListGroupsRequestData request, FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        return context.forwardRequest(header, request);
    }

    @Override
    CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header, ListGroupsResponseData response, FilterContext context,
                                                     AuthorizationFilter authorizationFilter) {
        List<Action> actions = response.groups().stream()
                .map(ListedGroup::groupId)
                .map(group -> new Action(DESCRIBE, group)).toList();
        return authorizationFilter.authorization(context, actions).thenCompose(authorizeResult -> {
            response.groups().removeIf(listedGroup -> authorizeResult.decision(DESCRIBE, listedGroup.groupId()) == Decision.DENY);
            return context.forwardResponse(header, response);
        });
    }
}
