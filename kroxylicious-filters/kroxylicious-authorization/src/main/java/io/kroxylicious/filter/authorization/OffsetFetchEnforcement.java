/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class OffsetFetchEnforcement extends ApiEnforcement<OffsetFetchRequestData, OffsetFetchResponseData> {
    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 0;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   OffsetFetchRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        // In v8 this request went from being about a single groups (and its topics)
        // to being about a batch of groups (and their individual topics)
        var isBatched = header.requestApiVersion() >= 8;

        List<Action> actions;
        if (isBatched) {
            actions = TopicResource.DESCRIBE.actionsOf(
                    request.groups().stream()
                            .flatMap(g -> g.topics().stream())
                            .map(OffsetFetchRequestData.OffsetFetchRequestTopics::name));
        }
        else {
            actions = TopicResource.DESCRIBE.actionsOf(
                    request.topics().stream()
                            .map(OffsetFetchRequestData.OffsetFetchRequestTopic::name));
        }
        return authorizationFilter.authorization(context, actions)
                .thenCompose(authorization -> {
                    if (isBatched) {
                        var t2 = authorization.partition(request.groups().stream()
                                .flatMap(g -> g.topics().stream())
                                .toList(), TopicResource.DESCRIBE, t -> t.name());
                        if (t2.get(Decision.ALLOW).isEmpty()) {
                            return context.requestFilterResultBuilder()
                                    .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                                    .completed();
                        }
                        else if (t2.get(Decision.DENY).isEmpty()) {
                            return context.forwardRequest(header, request);
                        }
                        else {
                            List<OffsetFetchResponseData.OffsetFetchResponseGroup> fm = new ArrayList<>();
                            for (var requestGroup : request.groups()) {
                                var allowed = requestGroup.topics().stream()
                                        .filter(t -> authorization.decision(TopicResource.DESCRIBE, t.name()) == Decision.ALLOW)
                                        .toList();
                                if (allowed.isEmpty()) {
                                    // TODO remove the whole group from the request
                                }
                                List<OffsetFetchResponseData.OffsetFetchResponseTopics> offsetFetchResponseTopics = t2
                                        .get(Decision.DENY).stream().map(x -> new OffsetFetchResponseData.OffsetFetchResponseTopics()
                                                .setName(x.name())
                                                .setPartitions(x.partitionIndexes().stream()
                                                        .map(i -> new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                                                .setPartitionIndex(i)
                                                                .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()))
                                                        .toList()))
                                        .toList();
                                fm.add(new OffsetFetchResponseData.OffsetFetchResponseGroup()
                                        .setGroupId(requestGroup.groupId())
                                        .setTopics(offsetFetchResponseTopics));
                            }

                            // savePartialResponse(header, (OffsetFetchResponseData response) -> {
                            // // TODO need to write the group merging eugh
                            // for (var g : response.groups()) {
                            // if (g.groupId()
                            // }
                            // response.groups().addAll(fm);
                            // return response;
                            // });
                            // g.setTopics(allowed);
                            return context.forwardRequest(header, request);
                        }
                    }
                    else {
                        var t1 = authorization.partition(request.topics(), TopicResource.DESCRIBE, t -> t.name());
                        if (t1.get(Decision.ALLOW).isEmpty()) {
                            return context.requestFilterResultBuilder()
                                    .errorResponse(header, request, Errors.TOPIC_AUTHORIZATION_FAILED.exception())
                                    .completed();
                        }
                        else if (t1.get(Decision.DENY).isEmpty()) {
                            return context.forwardRequest(header, request);
                        }
                        else {
                            var allowed = request.topics().stream().filter(t -> authorization.decision(TopicResource.DESCRIBE, t.name()) == Decision.ALLOW).toList();
                            var offsetFetchResponseTopics = t1.get(Decision.DENY).stream().map(x -> new OffsetFetchResponseData.OffsetFetchResponseTopic()
                                    .setName(x.name())
                                    .setPartitions(x.partitionIndexes().stream()
                                            .map(i -> new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                                    .setPartitionIndex(i)
                                                    .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()))
                                            .toList()))
                                    .toList();
                            authorizationFilter.pushInflightState(header, (OffsetFetchResponseData response) -> {
                                response.topics().addAll(offsetFetchResponseTopics);
                                return response;
                            });
                            request.setTopics(allowed);
                            return context.forwardRequest(header, request);
                        }
                    }
                });
    }
}
