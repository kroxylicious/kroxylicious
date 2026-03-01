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

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import static org.apache.kafka.common.config.ConfigResource.Type.GROUP;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

class AlterConfigsEnforcement extends ApiEnforcement<AlterConfigsRequestData, AlterConfigsResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 2;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, AlterConfigsRequestData request, FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<AlterConfigsResource> topicResources = ConfigResources.filter(request.resources().stream(), AlterConfigsResource::resourceType, TOPIC).toList();
        List<AlterConfigsResource> groupResources = ConfigResources.filter(request.resources().stream(), AlterConfigsResource::resourceType, GROUP).toList();
        if (topicResources.isEmpty() && groupResources.isEmpty()) {
            return context.forwardRequest(header, request);
        }
        else {
            Stream<Action> topicActions = topicResources.stream()
                    .map(re -> new Action(TopicResource.ALTER_CONFIGS, re.resourceName()));
            Stream<Action> groupActions = groupResources.stream()
                    .map(re -> new Action(GroupResource.ALTER_CONFIGS, re.resourceName()));
            List<Action> actions = Stream.concat(topicActions, groupActions).toList();
            return authorizationFilter.authorization(context, actions).thenCompose(result -> {
                if (result.denied().isEmpty()) {
                    return context.forwardRequest(header, request);
                }
                Map<Decision, List<AlterConfigsResource>> partitionedTopicResources = result.partition(topicResources, TopicResource.ALTER_CONFIGS,
                        AlterConfigsResource::resourceName);
                Map<Decision, List<AlterConfigsResource>> partitionedGroupResources = result.partition(groupResources, GroupResource.ALTER_CONFIGS,
                        AlterConfigsResource::resourceName);
                List<AlterConfigsResource> deniedTopics = partitionedTopicResources.get(Decision.DENY);
                List<AlterConfigsResource> deniedGroups = partitionedGroupResources.get(Decision.DENY);

                request.resources().removeAll(deniedTopics);
                request.resources().removeAll(deniedGroups);
                authorizationFilter.pushInflightState(header, (AlterConfigsResponseData alterConfigsResponseData) -> {
                    deniedTopics.forEach(describeConfigsResource -> {
                        alterConfigsResponseData.responses().add(failureResponse(Errors.TOPIC_AUTHORIZATION_FAILED, describeConfigsResource));
                    });
                    deniedGroups.forEach(describeConfigsResource -> {
                        alterConfigsResponseData.responses().add(failureResponse(Errors.GROUP_AUTHORIZATION_FAILED, describeConfigsResource));
                    });
                    return alterConfigsResponseData;
                });
                return context.forwardRequest(header, request);
            });
        }
    }

    private static AlterConfigsResponseData.AlterConfigsResourceResponse failureResponse(Errors error,
                                                                                         AlterConfigsResource requestResource) {
        AlterConfigsResponseData.AlterConfigsResourceResponse configResult = new AlterConfigsResponseData.AlterConfigsResourceResponse();
        configResult.setErrorCode(error.code());
        configResult.setErrorMessage(error.message());
        configResult.setResourceName(requestResource.resourceName());
        configResult.setResourceType(requestResource.resourceType());
        return configResult;
    }

    private static ConfigResource.Type resourceType(AlterConfigsResource resource) {
        return ConfigResource.Type.forId(resource.resourceType());
    }
}
