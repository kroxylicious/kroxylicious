/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.validation.validators.request.ProduceRequestValidationResult;
import io.kroxylicious.proxy.filter.validation.validators.request.ProduceRequestValidator;
import io.kroxylicious.proxy.filter.validation.validators.topic.PartitionValidationResult;
import io.kroxylicious.proxy.filter.validation.validators.topic.RecordValidationFailure;
import io.kroxylicious.proxy.filter.validation.validators.topic.TopicValidationResult;

/**
 * A Filter that is intended to validate some criteria about each topic-partition, preventing
 * invalid data being sent on to the broker. The response will contain an {@link Errors#INVALID_RECORD}
 * error code and an error message for each invalid topic partition.
 * <p>
 * Optionally supports forwarding partial data. If the request is non-transactional, the filter
 * can be configured to forward any valid topic-partitions to the broker, filtering out invalid ones.
 * Then when the ProduceResponse is intercepted then it can augment in failure messages for the invalid
 * topic-partitions.
 * </p>
 * <p>
 * Note: if all the topic partitions are invalid (or the request is transactional), a response is sent
 * back to the client without forwarding anything upstream, with all topic-partitions failed.
 * </p>
 */
public class RecordValidationFilter implements ProduceRequestFilter, ProduceResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordValidationFilter.class);
    private final ProduceRequestValidator validator;
    private final Map<Integer, ProduceRequestValidationResult> correlatedResults = new HashMap<>();

    /**
     * Construct a new ProduceValidationFilter
     *
     * @param validator validator to test ProduceRequests with
     */
    public RecordValidationFilter(ProduceRequestValidator validator) {
        if (validator == null) {
            throw new IllegalArgumentException("validator is null");
        }
        this.validator = validator;
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
        CompletionStage<ProduceRequestValidationResult> validationStage = validator.validateRequest(request);
        return validationStage.thenCompose(result -> {
            if (result.isAnyTopicPartitionInvalid()) {
                return handleInvalidTopicPartitions(request, context, result);
            }
            else {
                return context.forwardRequest(header, request);
            }
        });
    }

    private CompletionStage<RequestFilterResult> handleInvalidTopicPartitions(ProduceRequestData request, FilterContext context,
                                                                              ProduceRequestValidationResult result) {
        if (result.isAllTopicPartitionsInvalid()) {
            LOGGER.debug("all topic-partitions for request contained invalid data: {}", result);
            ProduceResponseData response = invalidateEntireRequest(request, result);
            return context.requestFilterResultBuilder().shortCircuitResponse(response).completed();
        }
        else {
            LOGGER.debug("some topic-partitions for transactional request with id: {}, contained invalid data: {}, invalidation entire request",
                    request.transactionalId(), result);
            ProduceResponseData response = invalidateEntireRequest(request, result);
            return context.requestFilterResultBuilder().shortCircuitResponse(response).completed();
        }
    }

    private static ProduceResponseData invalidateEntireRequest(ProduceRequestData request, ProduceRequestValidationResult produceRequestValidationResult) {
        ProduceResponseData response = new ProduceResponseData();
        ProduceResponseData.TopicProduceResponseCollection responseCollection = new ProduceResponseData.TopicProduceResponseCollection();
        request.topicData().forEach(topicProduceData -> {
            String topicName = topicProduceData.name();
            TopicValidationResult topicValidationResult = produceRequestValidationResult.topicResult(topicName);
            ProduceResponseData.TopicProduceResponse newElement = createInvalidatedTopicProduceResponse(topicName, topicProduceData, topicValidationResult);
            responseCollection.add(newElement);
        });
        response.setResponses(responseCollection);
        return response;
    }

    private static ProduceResponseData.TopicProduceResponse createInvalidatedTopicProduceResponse(String topicName,
                                                                                                  ProduceRequestData.TopicProduceData topicProduceData,
                                                                                                  TopicValidationResult topicValidationResult) {
        ProduceResponseData.TopicProduceResponse response = new ProduceResponseData.TopicProduceResponse();
        response.setName(topicName);
        List<ProduceResponseData.PartitionProduceResponse> responses = topicProduceData.partitionData().stream().map(partitionProduceData -> {
            PartitionValidationResult partitionResult = topicValidationResult.getPartitionResult(partitionProduceData.index());
            return createInvalidatedPartitionProduceResponse(partitionProduceData, partitionResult);
        }).toList();
        response.setPartitionResponses(responses);
        return response;
    }

    private static ProduceResponseData.PartitionProduceResponse createInvalidatedPartitionProduceResponse(ProduceRequestData.PartitionProduceData partitionProduceData,
                                                                                                          PartitionValidationResult partitionResult) {
        ProduceResponseData.PartitionProduceResponse produceResponse = new ProduceResponseData.PartitionProduceResponse();
        produceResponse.setIndex(partitionProduceData.index());
        produceResponse.setErrorCode(Errors.INVALID_RECORD.code());
        if (partitionResult.allRecordsValid()) {
            produceResponse.setErrorMessage("Invalid record in another topic-partition caused whole ProduceRequest to be invalidated");
        }
        else {
            for (RecordValidationFailure recordValidationFailure : partitionResult.recordValidationFailures()) {
                produceResponse.recordErrors().add(new ProduceResponseData.BatchIndexAndErrorMessage().setBatchIndex(recordValidationFailure.invalidIndex())
                        .setBatchIndexErrorMessage(recordValidationFailure.errorMessage()));
            }
            produceResponse.setErrorMessage(toErrorString(partitionResult.recordValidationFailures()));
        }
        return produceResponse;
    }

    private static String toErrorString(List<RecordValidationFailure> failures) {
        String failString = failures.stream().findFirst().map(RecordValidationFailure::errorMessage).orElse("Failure List Empty");
        return "Records in batch were invalid: [" + failString + "]";
    }

    @Override
    public CompletionStage<ResponseFilterResult> onProduceResponse(short apiVersion, ResponseHeaderData header, ProduceResponseData response,
                                                                   FilterContext context) {
        ProduceRequestValidationResult produceRequestValidationResult = correlatedResults.remove(header.correlationId());
        if (produceRequestValidationResult != null) {
            LOGGER.debug("augmenting invalid topic-partition details into response: {}", produceRequestValidationResult);
            augmentResponseWithInvalidTopicPartitions(response, produceRequestValidationResult);
            return context.forwardResponse(header, response);
        }
        else {
            return context.forwardResponse(header, response);
        }
    }

    private void augmentResponseWithInvalidTopicPartitions(ProduceResponseData response, ProduceRequestValidationResult produceRequestValidationResult) {
        produceRequestValidationResult.topicsWithInvalidPartitions().forEach(topicWithInvalidPartitions -> {
            ProduceResponseData.TopicProduceResponse topicProduceResponse = response.responses().find(topicWithInvalidPartitions.topicName());
            if (topicProduceResponse == null) {
                topicProduceResponse = new ProduceResponseData.TopicProduceResponse();
                topicProduceResponse.setName(topicWithInvalidPartitions.topicName());
                response.responses().add(topicProduceResponse);
            }
            augmentTopicProduceResponse(topicWithInvalidPartitions, topicProduceResponse);
        });
    }

    private static void augmentTopicProduceResponse(TopicValidationResult topicWithInvalidPartitions, ProduceResponseData.TopicProduceResponse topicProduceResponse) {
        topicWithInvalidPartitions.invalidPartitions().forEach(partitionValidationResult -> {
            ProduceResponseData.PartitionProduceResponse response = new ProduceResponseData.PartitionProduceResponse();
            response.setIndex(partitionValidationResult.index());
            for (RecordValidationFailure recordValidationFailure : partitionValidationResult.recordValidationFailures()) {
                response.recordErrors().add(new ProduceResponseData.BatchIndexAndErrorMessage().setBatchIndex(recordValidationFailure.invalidIndex())
                        .setBatchIndexErrorMessage(recordValidationFailure.errorMessage()));
            }
            response.setErrorCode(Errors.INVALID_RECORD.code());
            response.setErrorMessage(toErrorString(partitionValidationResult.recordValidationFailures()));
            topicProduceResponse.partitionResponses().add(response);
        });
    }

}
