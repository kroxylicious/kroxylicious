/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.validators.request;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.filter.validation.validators.request.ProduceRequestValidator.NamedTopicProduceData;
import io.kroxylicious.filter.validation.validators.topic.TopicValidationResult;
import io.kroxylicious.filter.validation.validators.topic.TopicValidator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RoutingProduceRequestValidatorTest {
    public static final String TOPIC_NAME = "my_topic";
    public static final Uuid TOPIC_ID = Uuid.randomUuid();
    @Mock
    TopicValidator validator;
    @Mock
    TopicValidationResult result;

    public static Stream<Arguments> routeWithTopicIdInData() {
        return Stream.of(Arguments.argumentSet("null name", new Object[]{ null }),
                Arguments.argumentSet("empty name", ""));
    }

    @Test
    void routeWithTopicNameInData() {
        ProduceRequestValidator requestValidator = RoutingProduceRequestValidator.builder()
                .appendValidatorForTopicPattern(Set.of(TOPIC_NAME), validator)
                .build();
        TopicProduceData produceData = new TopicProduceData();
        produceData.setName(TOPIC_NAME);
        CompletableFuture<TopicValidationResult> future = CompletableFuture.completedFuture(result);
        when(validator.validateTopicData(produceData, TOPIC_NAME)).thenReturn(future);
        when(result.topicName()).thenReturn(TOPIC_NAME);
        CompletionStage<ProduceRequestValidationResult> stage = requestValidator.validateRequest(List.of(new NamedTopicProduceData(TOPIC_NAME, produceData)));
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(produceResult -> {
            assertThat(produceResult.topicResult(TOPIC_NAME)).isSameAs(result);
        });
    }

    @MethodSource
    @ParameterizedTest
    void routeWithTopicIdInData(String topicName) {
        ProduceRequestValidator requestValidator = RoutingProduceRequestValidator.builder()
                .appendValidatorForTopicPattern(Set.of(TOPIC_NAME), validator)
                .build();
        TopicProduceData produceData = new TopicProduceData();
        produceData.setTopicId(TOPIC_ID);
        produceData.setName(topicName);
        CompletableFuture<TopicValidationResult> future = CompletableFuture.completedFuture(result);
        when(validator.validateTopicData(produceData, TOPIC_NAME)).thenReturn(future);
        when(result.topicName()).thenReturn(TOPIC_NAME);
        CompletionStage<ProduceRequestValidationResult> stage = requestValidator.validateRequest(List.of(new NamedTopicProduceData(TOPIC_NAME, produceData)));
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(produceResult -> {
            assertThat(produceResult.topicResult(TOPIC_NAME)).isSameAs(result);
        });
    }

    @Test
    void defaultIsAllValid() {
        ProduceRequestValidator requestValidator = RoutingProduceRequestValidator.builder()
                .build();
        ProduceRequestData request = new ProduceRequestData();
        TopicProduceData produceData = new TopicProduceData();
        produceData.setName(TOPIC_NAME);
        request.topicData().add(produceData);
        ProduceRequestData.PartitionProduceData partitionData = new ProduceRequestData.PartitionProduceData();
        partitionData.setIndex(0);
        produceData.partitionData().add(partitionData);
        CompletionStage<ProduceRequestValidationResult> stage = requestValidator.validateRequest(List.of(new NamedTopicProduceData(TOPIC_NAME, produceData)));
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(produceResult -> {
            TopicValidationResult actual = produceResult.topicResult(TOPIC_NAME);
            assertThat(actual.topicName()).isEqualTo(TOPIC_NAME);
            assertThat(actual.isAnyPartitionInvalid()).isFalse();
        });
    }

    @Test
    void overrideDefault() {
        ProduceRequestValidator requestValidator = RoutingProduceRequestValidator.builder()
                .setDefaultValidator(validator)
                .build();
        ProduceRequestData request = new ProduceRequestData();
        TopicProduceData produceData = new TopicProduceData();
        produceData.setName(TOPIC_NAME);
        request.topicData().add(produceData);
        CompletableFuture<TopicValidationResult> future = CompletableFuture.completedFuture(result);
        when(validator.validateTopicData(produceData, TOPIC_NAME)).thenReturn(future);
        when(result.topicName()).thenReturn(TOPIC_NAME);
        CompletionStage<ProduceRequestValidationResult> stage = requestValidator.validateRequest(List.of(new NamedTopicProduceData(TOPIC_NAME, produceData)));
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(produceResult -> {
            assertThat(produceResult.topicResult(TOPIC_NAME)).isSameAs(result);
        });
    }

}
