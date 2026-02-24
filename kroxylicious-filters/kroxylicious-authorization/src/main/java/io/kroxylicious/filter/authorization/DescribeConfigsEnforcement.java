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

import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResult;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import static org.apache.kafka.common.config.ConfigResource.Type.GROUP;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

class DescribeConfigsEnforcement extends ApiEnforcement<DescribeConfigsRequestData, DescribeClusterResponseData> {
    @Override
    short minSupportedVersion() {
        return 1;
    }

    @Override
    short maxSupportedVersion() {
        return 4;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, DescribeConfigsRequestData request, FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<DescribeConfigsResource> topicRequests = ConfigResources.filter(request.resources().stream(), DescribeConfigsResource::resourceType, TOPIC).toList();
        List<DescribeConfigsResource> groupRequests = ConfigResources.filter(request.resources().stream(), DescribeConfigsResource::resourceType, GROUP).toList();
        if (topicRequests.isEmpty() && groupRequests.isEmpty()) {
            return context.forwardRequest(header, request);
        }
        else {
            Stream<Action> topicActions = topicRequests.stream()
                    .map(resource -> new Action(TopicResource.DESCRIBE_CONFIGS, resource.resourceName()));
            Stream<Action> groupActions = groupRequests.stream()
                    .map(resource -> new Action(GroupResource.DESCRIBE_CONFIGS, resource.resourceName()));
            List<Action> actions = Stream.concat(groupActions, topicActions).toList();
            return authorizationFilter.authorization(context, actions).thenCompose(result -> {
                Map<Decision, List<DescribeConfigsResource>> partitionedTopics = result.partition(topicRequests, TopicResource.DESCRIBE_CONFIGS,
                        DescribeConfigsResource::resourceName);
                Map<Decision, List<DescribeConfigsResource>> partitionedGroups = result.partition(groupRequests, GroupResource.DESCRIBE_CONFIGS,
                        DescribeConfigsResource::resourceName);
                List<DescribeConfigsResource> deniedTopics = partitionedTopics.get(Decision.DENY);
                List<DescribeConfigsResource> deniedGroups = partitionedGroups.get(Decision.DENY);
                if (deniedTopics.isEmpty() && deniedGroups.isEmpty()) {
                    return context.forwardRequest(header, request);
                }
                else {
                    request.resources().removeAll(deniedTopics);
                    request.resources().removeAll(deniedGroups);
                    authorizationFilter.pushInflightState(header, (DescribeConfigsResponseData describeConfigsResponseData) -> {
                        deniedTopics.forEach(deniedTopic -> addResourceDescribeFailure(describeConfigsResponseData, deniedTopic, Errors.TOPIC_AUTHORIZATION_FAILED));
                        deniedGroups.forEach(deniedGroup -> addResourceDescribeFailure(describeConfigsResponseData, deniedGroup, Errors.GROUP_AUTHORIZATION_FAILED));
                        return describeConfigsResponseData;
                    });
                    return context.forwardRequest(header, request);
                }
            });
        }
    }

    private static void addResourceDescribeFailure(DescribeConfigsResponseData describeConfigsResponseData, DescribeConfigsResource describeConfigsResource,
                                                   Errors error) {
        DescribeConfigsResult configResult = new DescribeConfigsResult();
        configResult.setConfigs(List.of());
        configResult.setErrorCode(error.code());
        configResult.setErrorMessage(error.message());
        configResult.setResourceName(describeConfigsResource.resourceName());
        configResult.setResourceType(describeConfigsResource.resourceType());
        describeConfigsResponseData.results().add(configResult);
    }
}
