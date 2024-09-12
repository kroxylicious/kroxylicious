/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.filter.validation.validators.request.ProduceRequestValidationResult;
import io.kroxylicious.proxy.filter.validation.validators.request.ProduceRequestValidator;
import io.kroxylicious.proxy.filter.validation.validators.topic.PartitionValidationResult;
import io.kroxylicious.proxy.filter.validation.validators.topic.RecordValidationFailure;
import io.kroxylicious.proxy.filter.validation.validators.topic.TopicValidationResult;

import static org.apache.kafka.common.message.ProduceRequestData.HIGHEST_SUPPORTED_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecordValidationFilterTest {

    private static final String MY_TOPIC = "mytopic";

    @Mock
    private ProduceRequestValidator produceRequestValidator;

    @Mock
    private TopicValidationResult topicValidationResult;

    @Mock(strictness = LENIENT)
    private FilterContext context;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;

    @BeforeEach
    void setUp() {
        when(context.forwardRequest(any(RequestHeaderData.class), apiMessageCaptor.capture())).then(invocationOnMock -> {
            var filterResult = mock(RequestFilterResult.class);
            lenient().when(filterResult.message()).thenReturn(apiMessageCaptor.getValue());
            return CompletableFuture.completedFuture(filterResult);
        });

        when(context.requestFilterResultBuilder()).then(invocationOnMock -> {
            var builder = mock(RequestFilterResultBuilder.class);
            var filterResult = mock(RequestFilterResult.class);

            var closeOrTerminalStage = mock(CloseOrTerminalStage.class);
            lenient().when(closeOrTerminalStage.completed()).thenReturn(CompletableFuture.completedStage(filterResult));
            lenient().when(closeOrTerminalStage.build()).thenReturn(filterResult);

            when(builder.shortCircuitResponse(apiMessageCaptor.capture())).then(invocation -> {
                lenient().when(filterResult.shortCircuitResponse()).thenReturn(true);
                lenient().when(filterResult.message()).thenReturn(apiMessageCaptor.getValue());
                return closeOrTerminalStage;
            });
            return builder;
        });
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void rejectsNullValidator() {
        assertThatThrownBy(() -> new RecordValidationFilter(false, null))
                                                                         .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requestThatPassesValidationIsForwarded() {
        // Given
        var validator = new RecordValidationFilter(false, produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(false);

        var header = new RequestHeaderData();
        var request = buildProduceRequestData(
                new ProduceRequestData.TopicProduceData()
                                                         .setName(MY_TOPIC)
                                                         .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()))
        );

        when(produceRequestValidator.validateRequest(request)).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult)))
        );

        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, context);

        // Then
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .satisfies(rfr -> {
                              assertThat(rfr.shortCircuitResponse()).isFalse();
                              assertThat(rfr.message()).isEqualTo(request);
                          });
    }

    @Test
    void requestWithAllPartitionsFailedIsRejectedWithShortCircuitResponse() {
        // Given
        var validator = new RecordValidationFilter(false, produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(true);
        when(topicValidationResult.isAllPartitionsInvalid()).thenReturn(true);
        when(topicValidationResult.getPartitionResult(0)).thenReturn(new PartitionValidationResult(0, List.of(new RecordValidationFailure(0, "record error"))));

        var header = new RequestHeaderData();
        var request = buildProduceRequestData(
                new ProduceRequestData.TopicProduceData()
                                                         .setName(MY_TOPIC)
                                                         .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()))
        );
        when(produceRequestValidator.validateRequest(request)).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult)))
        );

        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, context);

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
    void requestWithSomePartitionsFailedIsIsForwarded() {
        // Given
        var validator = new RecordValidationFilter(true, produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(true);
        when(topicValidationResult.isAllPartitionsInvalid()).thenReturn(false);
        when(topicValidationResult.getPartitionResult(0)).thenReturn(new PartitionValidationResult(0, List.of(new RecordValidationFailure(0, "record error"))));
        when(topicValidationResult.getPartitionResult(1)).thenReturn(new PartitionValidationResult(1, List.of()));

        var header = new RequestHeaderData();
        final ProduceRequestData.PartitionProduceData partition1 = new ProduceRequestData.PartitionProduceData();
        final ProduceRequestData.PartitionProduceData partition2 = new ProduceRequestData.PartitionProduceData();
        partition2.setIndex(1);
        var request = buildProduceRequestData(
                new ProduceRequestData.TopicProduceData()
                                                         .setName(MY_TOPIC)
                                                         .setPartitionData(
                                                                 new ArrayList<>(
                                                                         List.of(
                                                                                 partition1,
                                                                                 partition2
                                                                         )
                                                                 )
                                                         )
        );
        when(produceRequestValidator.validateRequest(request)).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult)))
        );

        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, context);

        // Then
        assertThat(result)
                          .succeedsWithin(Duration.ofSeconds(1))
                          .satisfies(rfr -> {
                              assertThat(rfr.shortCircuitResponse()).isFalse();
                              assertThat(rfr.message())
                                                       .isInstanceOf(ProduceRequestData.class);

                              var prd = (ProduceRequestData) rfr.message();
                              assertThat(prd.topicData())
                                                         .singleElement()
                                                         .satisfies(tpr -> {
                                                             assertThat(tpr.name()).isEqualTo(MY_TOPIC);
                                                             assertThat(tpr.partitionData())
                                                                                            .singleElement()
                                                                                            .extracting(ProduceRequestData.PartitionProduceData::index)
                                                                                            .isEqualTo(1);
                                                         });
                          });
    }

    @Test
    void requestWithSomePartitionsFailedIsRejected() {
        // Given
        var validator = new RecordValidationFilter(false, produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(true);
        when(topicValidationResult.isAllPartitionsInvalid()).thenReturn(false);
        when(topicValidationResult.getPartitionResult(0)).thenReturn(new PartitionValidationResult(0, List.of(new RecordValidationFailure(0, "record error"))));
        when(topicValidationResult.getPartitionResult(1)).thenReturn(new PartitionValidationResult(1, List.of()));

        var header = new RequestHeaderData();
        final ProduceRequestData.PartitionProduceData partition1 = new ProduceRequestData.PartitionProduceData();
        final ProduceRequestData.PartitionProduceData partition2 = new ProduceRequestData.PartitionProduceData();
        partition2.setIndex(1);
        var request = buildProduceRequestData(
                new ProduceRequestData.TopicProduceData()
                                                         .setName(MY_TOPIC)
                                                         .setPartitionData(
                                                                 new ArrayList<>(
                                                                         List.of(
                                                                                 partition1,
                                                                                 partition2
                                                                         )
                                                                 )
                                                         )
        );
        when(produceRequestValidator.validateRequest(request)).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult)))
        );

        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, context);

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
                                                                                                 .matches(
                                                                                                         pr -> pr.errorMessage()
                                                                                                                 .contentEquals(
                                                                                                                         "Invalid record in another topic-partition caused whole ProduceRequest to be invalidated"
                                                                                                                 )
                                                                                                 );
                                                         });
                          });
    }

    @Test
    void requestWithAllPartitionsFailedIsRejectedWithShortCircuitResponseInTransaction() {
        // Given
        var validator = new RecordValidationFilter(true, produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(true);
        when(topicValidationResult.isAllPartitionsInvalid()).thenReturn(true);
        when(topicValidationResult.getPartitionResult(0)).thenReturn(new PartitionValidationResult(0, List.of(new RecordValidationFailure(0, "record error"))));

        var header = new RequestHeaderData();
        var request = buildProduceRequestData(
                Optional.of("testTransactionId"),
                new ProduceRequestData.TopicProduceData()
                                                         .setName(MY_TOPIC)
                                                         .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()))
        );
        when(produceRequestValidator.validateRequest(request)).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult)))
        );

        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, context);

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
    void requestWithSomePartitionsFailedIsRejectedWithShortCircuitResponseInTransaction() {
        // Given
        var validator = new RecordValidationFilter(true, produceRequestValidator);

        when(topicValidationResult.isAnyPartitionInvalid()).thenReturn(true);
        when(topicValidationResult.isAllPartitionsInvalid()).thenReturn(false);
        when(topicValidationResult.getPartitionResult(0)).thenReturn(new PartitionValidationResult(0, List.of(new RecordValidationFailure(0, "record error"))));

        var header = new RequestHeaderData();
        var request = buildProduceRequestData(
                Optional.of("testTransactionId"),
                new ProduceRequestData.TopicProduceData()
                                                         .setName(MY_TOPIC)
                                                         .setPartitionData(
                                                                 List.of(
                                                                         new ProduceRequestData.PartitionProduceData(),
                                                                         new ProduceRequestData.PartitionProduceData()
                                                                 )
                                                         )
        );
        when(produceRequestValidator.validateRequest(request)).thenReturn(
                CompletableFuture.completedStage(new ProduceRequestValidationResult(Map.of(MY_TOPIC, topicValidationResult)))
        );

        // When
        var result = validator.onProduceRequest(HIGHEST_SUPPORTED_VERSION, header, request, context);

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
