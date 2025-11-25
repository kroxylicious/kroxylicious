/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import static org.apache.kafka.common.resource.ResourceType.TOPIC;

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
        List<AlterConfigsResource> topicRequests = request.resources().stream()
                .filter(resource -> ResourceType.fromCode(resource.resourceType()) == TOPIC).toList();
        if (topicRequests.isEmpty()) {
            return context.forwardRequest(header, request);
        }
        else {
            List<Action> actions = topicRequests.stream()
                    .map(re -> new Action(TopicResource.ALTER_CONFIGS, re.resourceName())).toList();
            return authorizationFilter.authorization(context, actions).thenCompose(result -> {
                Map<Decision, List<AlterConfigsResource>> partitioned = result.partition(topicRequests, TopicResource.ALTER_CONFIGS,
                        AlterConfigsResource::resourceName);
                List<AlterConfigsResource> denied = partitioned.get(Decision.DENY);
                if (denied.isEmpty()) {
                    return context.forwardRequest(header, request);
                }
                else {
                    request.resources().removeAll(denied);
                    authorizationFilter.pushInflightState(header, (IncrementalAlterConfigsResponseData d) -> {
                        denied.forEach(describeConfigsResource -> {
                            IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse configResult = new IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse();
                            configResult.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
                            configResult.setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message());
                            configResult.setResourceName(describeConfigsResource.resourceName());
                            configResult.setResourceType(describeConfigsResource.resourceType());
                            d.responses().add(configResult);
                        });
                        return d;
                    });
                    return context.forwardRequest(header, request);
                }
            });
        }
    }
}
