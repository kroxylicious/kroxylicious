/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsPartition;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteRecordsResponse;

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
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, DeleteRecordsRequestData request, FilterContext context,
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
                                partitionResult.setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK);
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
