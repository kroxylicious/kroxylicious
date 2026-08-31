/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.DeleteRecordsRequestData;
import io.kroxylicious.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsPartition;
import io.kroxylicious.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic;
import io.kroxylicious.kafka.common.message.DeleteRecordsResponseData;
import io.kroxylicious.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import io.kroxylicious.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

class DeleteRecordsEnforcement extends ApiEnforcement<DeleteRecordsRequestData, DeleteRecordsResponseData> {
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
                                                   DeleteRecordsRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        if (request.topics() == null || request.topics().isEmpty()) {
            return context.forwardRequest(header, request);
        }
        else {
            List<Action> actions = request.topics().stream()
                    .map(topic -> new Action(TopicResource.DELETE, topic.name())).toList();
            return authorizationFilter.authorization(context, actions).thenCompose(result -> {
                var partitioned = result.partition(request.topics(), TopicResource.DELETE,
                        DeleteRecordsTopic::name);
                List<DeleteRecordsTopic> denied = partitioned.get(Decision.DENY);
                if (!denied.isEmpty()) {
                    request.topics().removeAll(denied);
                    authorizationFilter.pushInflightState(header, (DeleteRecordsResponseData response) -> {
                        denied.forEach(deleteRecordsTopic -> {
                            DeleteRecordsTopicResult deleteResult = new DeleteRecordsTopicResult();
                            deleteResult.setName(deleteRecordsTopic.name());
                            for (DeleteRecordsPartition partition : deleteRecordsTopic.partitions()) {
                                DeleteRecordsPartitionResult partitionResult = new DeleteRecordsPartitionResult();
                                partitionResult.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
                                partitionResult.setPartitionIndex(partition.partitionIndex());
                                partitionResult.setLowWatermark(-1L); // Invalid low watermark
                                deleteResult.partitions().add(partitionResult);
                            }
                            response.topics().mustAdd(deleteResult);
                        });
                        return response;
                    });
                }
                return context.forwardRequest(header, request);
            });
        }
    }
}
