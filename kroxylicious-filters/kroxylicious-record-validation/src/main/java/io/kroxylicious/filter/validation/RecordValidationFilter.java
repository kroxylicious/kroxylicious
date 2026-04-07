/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.filter.validation.validators.request.ProduceRequestValidationResult;
import io.kroxylicious.filter.validation.validators.request.ProduceRequestValidator;
import io.kroxylicious.filter.validation.validators.request.ProduceRequestValidator.NamedTopicProduceData;
import io.kroxylicious.filter.validation.validators.topic.PartitionValidationResult;
import io.kroxylicious.filter.validation.validators.topic.RecordValidationFailure;
import io.kroxylicious.filter.validation.validators.topic.TopicValidationResult;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;

/**
 * The filter intercepts the produce requests and subject the records contained within them to validation. If the
 * validation fails, the whole produce request is rejected and the producing application receives an error
 * response {@link Errors#INVALID_RECORD}.  The broker does not receive rejected produce requests.
 */
class RecordValidationFilter implements ProduceRequestFilter, ProduceResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordValidationFilter.class);
    private final ProduceRequestValidator validator;
    private final Map<Integer, ProduceRequestValidationResult> correlatedResults = new HashMap<>();

    /**
     * Construct a new ProduceValidationFilter
     *
     * @param validator validator to test ProduceRequests with
     */
    RecordValidationFilter(ProduceRequestValidator validator) {
        if (validator == null) {
            throw new IllegalArgumentException("validator is null");
        }
        this.validator = validator;
    }

    private static String topicName(TopicProduceData topicProduceData, Map<Uuid, String> topicNames) {
        if (topicProduceData.name() != null && !topicProduceData.name().isEmpty()) {
            return topicProduceData.name();
        }
        else {
            String mappedName = topicNames.get(topicProduceData.topicId());
            if (mappedName == null) {
                throw new IllegalStateException(
                        "topic name not found for TopicProduceData with name: '" + topicProduceData.name() + "' and topicId: '" + topicProduceData.topicId() + "'");
            }
            return mappedName;
        }
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
        List<Uuid> uuids = extractTopicIds(request);
        return context.topicNames(uuids).thenCompose(topicNameMapping -> {
            if (topicNameMapping.anyFailures()) {
                return context.requestFilterResultBuilder().errorResponse(header, request, Errors.UNKNOWN_TOPIC_ID.exception()).completed();
            }
            List<NamedTopicProduceData> namedTopicProduceData = request.topicData().stream()
                    .map(data -> namedData(topicNameMapping, data)).toList();
            CompletionStage<ProduceRequestValidationResult> validationStage = validator.validateRequest(namedTopicProduceData);
            return validationStage.thenCompose(result -> {
                if (result.isAnyTopicPartitionInvalid()) {
                    return handleInvalidTopicPartitions(namedTopicProduceData, context, result);
                }
                else {
                    return context.forwardRequest(header, request);
                }
            });
        });
    }

    private static NamedTopicProduceData namedData(TopicNameMapping topicNameMapping,
                                                   TopicProduceData data) {
        return new NamedTopicProduceData(topicName(data, topicNameMapping.topicNames()), data);
    }

    private static List<Uuid> extractTopicIds(ProduceRequestData request) {
        return request.topicData().stream()
                .map(TopicProduceData::topicId)
                .filter(uuid -> !Uuid.ZERO_UUID.equals(uuid))
                .toList();
    }

    private CompletionStage<RequestFilterResult> handleInvalidTopicPartitions(List<NamedTopicProduceData> requestTopicData,
                                                                              FilterContext context,
                                                                              ProduceRequestValidationResult result) {
        LOGGER.atDebug()
                .addKeyValue("validationResult", result)
                .log("At least one topic-partitions with the request contained invalid records. Produce request will be rejected");
        ProduceResponseData response = invalidateEntireRequest(requestTopicData, result);
        return context.requestFilterResultBuilder().shortCircuitResponse(response).completed();
    }

    private static ProduceResponseData invalidateEntireRequest(List<NamedTopicProduceData> requestTopicData,
                                                               ProduceRequestValidationResult produceRequestValidationResult) {
        ProduceResponseData response = new ProduceResponseData();
        ProduceResponseData.TopicProduceResponseCollection responseCollection = new ProduceResponseData.TopicProduceResponseCollection();
        requestTopicData.forEach(topicProduceData -> {
            TopicValidationResult topicValidationResult = produceRequestValidationResult.topicResult(topicProduceData.topicName());
            ProduceResponseData.TopicProduceResponse newElement = createInvalidatedTopicProduceResponse(topicProduceData.data(), topicValidationResult);
            responseCollection.add(newElement);
        });
        response.setResponses(responseCollection);
        return response;
    }

    private static ProduceResponseData.TopicProduceResponse createInvalidatedTopicProduceResponse(TopicProduceData topicProduceData,
                                                                                                  TopicValidationResult topicValidationResult) {
        ProduceResponseData.TopicProduceResponse response = new ProduceResponseData.TopicProduceResponse();
        // we preserve the request topic identification mechanism here, one of name or topicId will be non-null
        response.setName(topicProduceData.name());
        response.setTopicId(topicProduceData.topicId());
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
            LOGGER.atDebug()
                    .addKeyValue("validationResult", produceRequestValidationResult)
                    .log("Augmenting invalid topic-partition details into response");
            augmentResponseWithInvalidTopicPartitions(response, produceRequestValidationResult);
            return context.forwardResponse(header, response);
        }
        else {
            return context.forwardResponse(header, response);
        }
    }

    private void augmentResponseWithInvalidTopicPartitions(ProduceResponseData response, ProduceRequestValidationResult produceRequestValidationResult) {
        produceRequestValidationResult.topicsWithInvalidPartitions().forEach(topicWithInvalidPartitions -> {
            ProduceResponseData.TopicProduceResponse topicProduceResponse = response.responses().find(topicWithInvalidPartitions.topicName(), null);
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
