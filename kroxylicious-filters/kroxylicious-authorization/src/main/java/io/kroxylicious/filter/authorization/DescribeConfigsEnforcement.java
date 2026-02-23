/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

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
        if (topicRequests.isEmpty()) {
            return context.forwardRequest(header, request);
        }
        else {
            List<Action> actions = topicRequests.stream()
                    .map(resource -> new Action(TopicResource.DESCRIBE_CONFIGS, resource.resourceName())).toList();
            return authorizationFilter.authorization(context, actions).thenCompose(result -> {
                Map<Decision, List<DescribeConfigsResource>> partitioned = result.partition(topicRequests, TopicResource.DESCRIBE_CONFIGS,
                        DescribeConfigsResource::resourceName);
                List<DescribeConfigsResource> denied = partitioned.get(Decision.DENY);
                if (denied.isEmpty()) {
                    return context.forwardRequest(header, request);
                }
                else {
                    request.resources().removeAll(denied);
                    authorizationFilter.pushInflightState(header, (DescribeConfigsResponseData describeConfigsResponseData) -> {
                        denied.forEach(describeConfigsResource -> {
                            DescribeConfigsResult configResult = new DescribeConfigsResult();
                            configResult.setConfigs(List.of());
                            configResult.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
                            configResult.setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message());
                            configResult.setResourceName(describeConfigsResource.resourceName());
                            configResult.setResourceType(describeConfigsResource.resourceType());
                            describeConfigsResponseData.results().add(configResult);
                        });
                        return describeConfigsResponseData;
                    });
                    return context.forwardRequest(header, request);
                }
            });
        }
    }
}
