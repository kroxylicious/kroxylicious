/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.filter.validation.validators.request.ProduceRequestValidationResult;
import io.kroxylicious.filter.validation.validators.request.ProduceRequestValidator;
import io.kroxylicious.filter.validation.validators.request.ProduceRequestValidator.NamedTopicProduceData;
import io.kroxylicious.filter.validation.validators.topic.PartitionValidationResult;
import io.kroxylicious.filter.validation.validators.topic.RecordValidationFailure;
import io.kroxylicious.filter.validation.validators.topic.TopicValidationResult;
import io.kroxylicious.testing.filter.assertj.MockFilterContextAssert;
import io.kroxylicious.testing.filter.context.MockFilterContext;

import static org.apache.kafka.common.message.ProduceRequestData.HIGHEST_SUPPORTED_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecordValidationFilterTest {

    private static final String MY_TOPIC = "mytopic";
    public static final Uuid TOPIC_ID = Uuid.randomUuid();

    @Mock
    private ProduceRequestValidator produceRequestValidator;

    @Mock
    private TopicValidationResult topicValidationResult;

    @SuppressWarnings("DataFlowIssue")
    @Test
    void rejectsNullValidator() {
        assertThatThrownBy(() -> new RecordValidationFilter(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requestThatPassesValidationWithTopicNamesIsForwarded() {
        // Given
        var validator = new RecordValidationFilter(produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(false);

        var header = new RequestHeaderData();
        ProduceRequestData.TopicProduceData produceData = new ProduceRequestData.TopicProduceData()
                .setName(MY_TOPIC)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()));
        var request = buildProduceRequestData(produceData);

        when(produceRequestValidator.validateRequest(List.of(new NamedTopicProduceData(MY_TOPIC, produceData)))).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult))));

        MockFilterContext mockFilterContext = MockFilterContext.builder(header, request).build();
        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, mockFilterContext);

        // Then
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.shortCircuitResponse()).isFalse();
                    assertThat(rfr.message()).isEqualTo(request);
                });
    }

    @Test
    void requestThatPassesValidationWithTopicIdsIsForwarded() {
        // Given
        var validator = new RecordValidationFilter(produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(false);

        var header = new RequestHeaderData();
        ProduceRequestData.TopicProduceData produceData = new ProduceRequestData.TopicProduceData()
                .setTopicId(TOPIC_ID)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()));
        var request = buildProduceRequestData(produceData);

        when(produceRequestValidator.validateRequest(List.of(new NamedTopicProduceData(MY_TOPIC, produceData)))).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult))));

        MockFilterContext mockFilterContext = MockFilterContext.builder(header, request).withTopicName(TOPIC_ID, MY_TOPIC).build();
        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, mockFilterContext);

        // Then
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.shortCircuitResponse()).isFalse();
                    assertThat(rfr.message()).isEqualTo(request);
                });
    }

    @Test
    void topicIdLookupFailure() {
        // Given
        var validator = new RecordValidationFilter(produceRequestValidator);

        var header = new RequestHeaderData();
        var request = buildProduceRequestData(new ProduceRequestData.TopicProduceData()
                .setTopicId(TOPIC_ID)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData())));
        // context will return mapping error as we have not configured topic ids
        MockFilterContext mockFilterContext = MockFilterContext.builder(header, request).build();
        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, mockFilterContext);

        // Then
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    MockFilterContextAssert.assertThat(rfr)
                            .isErrorResponse()
                            .errorResponse().isInstanceOf(UnknownTopicIdException.class);
                });
    }

    @Test
    void requestWithAllPartitionsFailedIsRejectedWithShortCircuitResponse() {
        // Given
        var validator = new RecordValidationFilter(produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(true);
        when(topicValidationResult.getPartitionResult(0)).thenReturn(new PartitionValidationResult(0, List.of(new RecordValidationFailure(0, "record error"))));

        var header = new RequestHeaderData();
        ProduceRequestData.TopicProduceData produceData = new ProduceRequestData.TopicProduceData()
                .setName(MY_TOPIC)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()));
        var request = buildProduceRequestData(produceData);
        when(produceRequestValidator.validateRequest(List.of(new NamedTopicProduceData(MY_TOPIC, produceData)))).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult))));
        MockFilterContext mockFilterContext = MockFilterContext.builder(header, request).build();

        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, mockFilterContext);

        // Then
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.shortCircuitResponse()).isTrue();
                    assertThat(rfr.message())
                            .isInstanceOf(ProduceResponseData.class);

                    var prd = (ProduceResponseData) rfr.message();
                    assertThat(prd.responses())
                            .singleElement()
                            .satisfies(tpr -> {
                                assertThat(tpr.name()).isEqualTo(MY_TOPIC);
                                assertThat(tpr.partitionResponses())
                                        .singleElement()
                                        .matches(pr -> pr.errorCode() == Errors.INVALID_RECORD.code());
                            });
                });
    }

    @Test
    void requestWithSomePartitionsFailedIsRejected() {
        // Given
        var validator = new RecordValidationFilter(produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(true);
        when(topicValidationResult.getPartitionResult(0)).thenReturn(new PartitionValidationResult(0, List.of(new RecordValidationFailure(0, "record error"))));
        when(topicValidationResult.getPartitionResult(1)).thenReturn(new PartitionValidationResult(1, List.of()));

        var header = new RequestHeaderData();
        final ProduceRequestData.PartitionProduceData partition1 = new ProduceRequestData.PartitionProduceData();
        final ProduceRequestData.PartitionProduceData partition2 = new ProduceRequestData.PartitionProduceData();
        partition2.setIndex(1);
        ProduceRequestData.TopicProduceData produceData = new ProduceRequestData.TopicProduceData()
                .setName(MY_TOPIC)
                .setPartitionData(
                        new ArrayList<>(List.of(
                                partition1,
                                partition2)));
        var request = buildProduceRequestData(produceData);
        when(produceRequestValidator.validateRequest(List.of(new NamedTopicProduceData(MY_TOPIC, produceData)))).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult))));
        MockFilterContext mockFilterContext = MockFilterContext.builder(header, request).build();

        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, mockFilterContext);

        // Then
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.shortCircuitResponse()).isTrue();
                    assertThat(rfr.message())
                            .isInstanceOf(ProduceResponseData.class);

                    var prd = (ProduceResponseData) rfr.message();
                    assertThat(prd.responses())
                            .singleElement()
                            .satisfies(tpr -> {
                                assertThat(tpr.name()).isEqualTo(MY_TOPIC);
                                assertThat(tpr.partitionResponses())
                                        .element(0)
                                        .matches(pr -> pr.errorCode() == Errors.INVALID_RECORD.code());
                                assertThat(tpr.partitionResponses())
                                        .element(1)
                                        .matches(pr -> pr.errorMessage()
                                                .contentEquals("Invalid record in another topic-partition caused whole ProduceRequest to be invalidated"));
                            });
                });
    }

    @Test
    void requestWithAllPartitionsFailedIsRejectedWithShortCircuitResponseInTransaction() {
        // Given
        var validator = new RecordValidationFilter(produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(true);
        when(topicValidationResult.getPartitionResult(0)).thenReturn(new PartitionValidationResult(0, List.of(new RecordValidationFailure(0, "record error"))));

        var header = new RequestHeaderData();
        ProduceRequestData.TopicProduceData produceData = new ProduceRequestData.TopicProduceData()
                .setName(MY_TOPIC)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()));
        var request = buildProduceRequestData(Optional.of("testTransactionId"), produceData);
        when(produceRequestValidator.validateRequest(List.of(new NamedTopicProduceData(MY_TOPIC, produceData)))).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult))));
        MockFilterContext mockFilterContext = MockFilterContext.builder(header, request).build();

        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, mockFilterContext);

        // Then
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.shortCircuitResponse()).isTrue();
                    assertThat(rfr.message())
                            .isInstanceOf(ProduceResponseData.class);

                    var prd = (ProduceResponseData) rfr.message();
                    assertThat(prd.responses())
                            .singleElement()
                            .satisfies(tpr -> {
                                assertThat(tpr.name()).isEqualTo(MY_TOPIC);
                                assertThat(tpr.partitionResponses())
                                        .singleElement()
                                        .matches(pr -> pr.errorCode() == Errors.INVALID_RECORD.code());
                            });
                });
    }

    @Test
    void requestUsingTopicNameWithSomePartitionsFailedIsRejectedWithShortCircuitResponseInTransaction() {
        // Given
        var validator = new RecordValidationFilter(produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(true);
        when(topicValidationResult.getPartitionResult(0)).thenReturn(new PartitionValidationResult(0, List.of(new RecordValidationFailure(0, "record error"))));

        var header = new RequestHeaderData();
        ProduceRequestData.TopicProduceData produceData = new ProduceRequestData.TopicProduceData()
                .setName(MY_TOPIC)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData(),
                        new ProduceRequestData.PartitionProduceData()));
        var request = buildProduceRequestData(Optional.of("testTransactionId"), produceData);
        when(produceRequestValidator.validateRequest(List.of(new NamedTopicProduceData(MY_TOPIC, produceData)))).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult))));
        MockFilterContext mockFilterContext = MockFilterContext.builder(header, request).build();

        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, mockFilterContext);

        // Then
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.shortCircuitResponse()).isTrue();
                    assertThat(rfr.message())
                            .isInstanceOf(ProduceResponseData.class);

                    var prd = (ProduceResponseData) rfr.message();
                    assertThat(prd.responses())
                            .singleElement()
                            .satisfies(tpr -> {
                                assertThat(tpr.name()).isEqualTo(MY_TOPIC);
                                assertThat(tpr.topicId()).isEqualTo(Uuid.ZERO_UUID);
                                assertThat(tpr.partitionResponses())
                                        .allMatch(pr -> pr.errorCode() == Errors.INVALID_RECORD.code());
                            });
                });
    }

    @Test
    void requestUsingTopicIdWithSomePartitionsFailedIsRejectedWithShortCircuitResponseInTransaction() {
        // Given
        var validator = new RecordValidationFilter(produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(true);
        when(topicValidationResult.getPartitionResult(0)).thenReturn(new PartitionValidationResult(0, List.of(new RecordValidationFailure(0, "record error"))));

        var header = new RequestHeaderData();
        ProduceRequestData.TopicProduceData produceData = new ProduceRequestData.TopicProduceData()
                .setTopicId(TOPIC_ID)
                .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData(),
                        new ProduceRequestData.PartitionProduceData()));
        var request = buildProduceRequestData(Optional.of("testTransactionId"), produceData);
        when(produceRequestValidator.validateRequest(List.of(new NamedTopicProduceData(MY_TOPIC, produceData)))).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult))));
        MockFilterContext mockFilterContext = MockFilterContext.builder(header, request).withTopicName(TOPIC_ID, MY_TOPIC).build();

        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, mockFilterContext);

        // Then
        assertThat(result)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.shortCircuitResponse()).isTrue();
                    assertThat(rfr.message())
                            .isInstanceOf(ProduceResponseData.class);

                    var prd = (ProduceResponseData) rfr.message();
                    assertThat(prd.responses())
                            .singleElement()
                            .satisfies(tpr -> {
                                assertThat(tpr.topicId()).isEqualTo(TOPIC_ID);
                                assertThat(tpr.name()).isEmpty();
                                assertThat(tpr.partitionResponses())
                                        .allMatch(pr -> pr.errorCode() == Errors.INVALID_RECORD.code());
                            });
                });
    }

    private static ProduceRequestData buildProduceRequestData(ProduceRequestData.TopicProduceData... produceData) {
        return buildProduceRequestData(Optional.empty(), produceData);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static ProduceRequestData buildProduceRequestData(Optional<String> transactionId, ProduceRequestData.TopicProduceData... produceData) {
        var data = new ProduceRequestData();
        data.topicData().addAll(Arrays.asList(produceData));
        transactionId.ifPresent(data::setTransactionalId);
        return data;

    }
}
