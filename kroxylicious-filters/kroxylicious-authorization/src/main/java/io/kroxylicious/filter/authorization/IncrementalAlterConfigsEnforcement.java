/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import static org.apache.kafka.common.config.ConfigResource.Type.GROUP;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

class IncrementalAlterConfigsEnforcement extends ApiEnforcement<IncrementalAlterConfigsRequestData, IncrementalAlterConfigsResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 1;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, IncrementalAlterConfigsRequestData request, FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<AlterConfigsResource> topicRequests = ConfigResources.filter(request.resources().stream(), AlterConfigsResource::resourceType, TOPIC).toList();
        List<AlterConfigsResource> groupRequests = ConfigResources.filter(request.resources().stream(), AlterConfigsResource::resourceType, GROUP).toList();
        if (topicRequests.isEmpty() && groupRequests.isEmpty()) {
            return context.forwardRequest(header, request);
        }
        else {
            Stream<Action> topicActions = topicRequests.stream()
                    .map(re -> new Action(TopicResource.ALTER_CONFIGS, re.resourceName()));
            Stream<Action> groupActions = groupRequests.stream()
                    .map(re -> new Action(GroupResource.ALTER_CONFIGS, re.resourceName()));
            List<Action> actions = Stream.concat(groupActions, topicActions).toList();
            return authorizationFilter.authorization(context, actions).thenCompose(result -> {
                Map<Decision, List<AlterConfigsResource>> partitionedTopicResources = result.partition(topicRequests, TopicResource.ALTER_CONFIGS,
                        AlterConfigsResource::resourceName);
                Map<Decision, List<AlterConfigsResource>> partitionedGroupResources = result.partition(groupRequests, GroupResource.ALTER_CONFIGS,
                        AlterConfigsResource::resourceName);
                List<AlterConfigsResource> deniedTopicResources = partitionedTopicResources.get(Decision.DENY);
                List<AlterConfigsResource> deniedGroupResources = partitionedGroupResources.get(Decision.DENY);
                if (deniedTopicResources.isEmpty() && deniedGroupResources.isEmpty()) {
                    return context.forwardRequest(header, request);
                }
                else {
                    request.resources().removeAll(deniedTopicResources);
                    request.resources().removeAll(deniedGroupResources);
                    authorizationFilter.pushInflightState(header, (IncrementalAlterConfigsResponseData response) -> {
                        deniedTopicResources.forEach(deniedTopicResource -> {
                            addResourceFailure(response, deniedTopicResource, Errors.TOPIC_AUTHORIZATION_FAILED);
                        });
                        deniedGroupResources.forEach(deniedGroupResource -> {
                            addResourceFailure(response, deniedGroupResource, Errors.GROUP_AUTHORIZATION_FAILED);
                        });
                        return response;
                    });
                    return context.forwardRequest(header, request);
                }
            });
        }
    }

    private static void addResourceFailure(IncrementalAlterConfigsResponseData response, AlterConfigsResource describeConfigsResource, Errors error) {
        IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse configResult = new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse();
        configResult.setErrorCode(error.code());
        configResult.setErrorMessage(error.message());
        configResult.setResourceName(describeConfigsResource.resourceName());
        configResult.setResourceType(describeConfigsResource.resourceType());
        response.responses().add(configResult);
    }
}
