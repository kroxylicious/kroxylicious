/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TopicNameLearningFilterTest {

    private static final CompletableFuture<ResponseFilterResult> FORWARD_FUTURE = CompletableFuture.completedFuture(null);
    private static final Uuid TOPIC_UUID = Uuid.randomUuid();
    private static final String TOPIC_NAME = "topic";

    @Mock
    VirtualClusterModel virtualClusterModel;

    @Mock
    FilterContext filterContext;

    TopicNameLearningFilter topicNameLearningFilter;

    @BeforeEach
    void beforeEach() {
        topicNameLearningFilter = new TopicNameLearningFilter(virtualClusterModel);
    }

    @Test
    void onlyInterceptMetadataThatCanCarryTopicIds() {
        IntStream tooOld = IntStream.range(ApiKeys.METADATA.oldestVersion(), 12);
        IntStream hasTopicIds = IntStream.range(12, ApiKeys.METADATA.latestVersion(true));
        tooOld.forEach(i -> assertThat(topicNameLearningFilter.shouldHandleMetadataResponse((short) i)).isFalse());
        hasTopicIds.forEach(i -> assertThat(topicNameLearningFilter.shouldHandleMetadataResponse((short) i)).isTrue());
    }

    @Test
    void onlyInterceptCreateTopicResponseVersionsThatCanCarryTopicIds() {
        IntStream tooOld = IntStream.range(ApiKeys.CREATE_TOPICS.oldestVersion(), 7);
        IntStream hasTopicIds = IntStream.range(7, ApiKeys.CREATE_TOPICS.latestVersion(true));
        tooOld.forEach(i -> assertThat(topicNameLearningFilter.shouldHandleCreateTopicsResponse((short) i)).isFalse());
        hasTopicIds.forEach(i -> assertThat(topicNameLearningFilter.shouldHandleCreateTopicsResponse((short) i)).isTrue());
    }

    static Stream<MetadataResponseData> learnsNothingFromMetadataAndForwards() {
        MetadataResponseData empty = new MetadataResponseData();
        MetadataResponseData nullTopic = new MetadataResponseData().setTopics(null);
        MetadataResponseData topicWithDefaultName = metadataWithSingleTopic(t -> {
            t.setTopicId(TOPIC_UUID);
        });
        MetadataResponseData topicWithNullName = metadataWithSingleTopic(t -> {
            t.setTopicId(TOPIC_UUID);
            t.setName(null);
        });
        MetadataResponseData topicWithDefaultTopicId = metadataWithSingleTopic(t -> {
            t.setName(TOPIC_NAME);
        });
        MetadataResponseData topicWithNullTopidId = metadataWithSingleTopic(t -> {
            t.setTopicId(null);
            t.setName(TOPIC_NAME);
        });
        return Stream.of(empty, nullTopic, topicWithDefaultName, topicWithNullName, topicWithDefaultTopicId, topicWithNullTopidId);
    }

    public static Stream<CreateTopicsResponseData> learnsNothingFromCreateTopicResponseAndForwards() {
        CreateTopicsResponseData empty = new CreateTopicsResponseData();
        CreateTopicsResponseData nullTopics = new CreateTopicsResponseData().setTopics(null);
        CreateTopicsResponseData topicWithDefaultName = createTopicWithSingleTopic(t -> {
            t.setTopicId(TOPIC_UUID);
        });
        CreateTopicsResponseData topicWithNullName = createTopicWithSingleTopic(t -> {
            t.setTopicId(TOPIC_UUID);
            t.setName(null);
        });
        CreateTopicsResponseData topicWithDefaultTopicId = createTopicWithSingleTopic(t -> {
            t.setName(TOPIC_NAME);
        });
        CreateTopicsResponseData topicWithNullTopidId = createTopicWithSingleTopic(t -> {
            t.setTopicId(null);
            t.setName(TOPIC_NAME);
        });
        return Stream.of(empty, nullTopics, topicWithDefaultName, topicWithNullName, topicWithDefaultTopicId, topicWithNullTopidId);
    }

    @ParameterizedTest
    @MethodSource
    void learnsNothingFromMetadataAndForwards(MetadataResponseData data) {
        when(filterContext.forwardResponse(any(), any())).thenReturn(FORWARD_FUTURE);
        CompletionStage<ResponseFilterResult> resultStage = onMetadataResponse(data);
        assertThat(resultStage).isSameAs(FORWARD_FUTURE);
        verify(virtualClusterModel, never()).rememberTopicIdMapping(any(), any());
    }

    @ParameterizedTest
    @MethodSource
    void learnsNothingFromCreateTopicResponseAndForwards(CreateTopicsResponseData data) {
        when(filterContext.forwardResponse(any(), any())).thenReturn(FORWARD_FUTURE);
        CompletionStage<ResponseFilterResult> resultStage = onCreateTopicsResponse(data);
        assertThat(resultStage).isSameAs(FORWARD_FUTURE);
        verify(virtualClusterModel, never()).rememberTopicIdMapping(any(), any());
    }

    @Test
    void learnsTopicNameFromMetadataAndForwards() {
        when(filterContext.forwardResponse(any(), any())).thenReturn(FORWARD_FUTURE);
        MetadataResponseData metadataResponseData = metadataWithSingleTopic(t -> {
            t.setName(TOPIC_NAME);
            t.setTopicId(TOPIC_UUID);
        });
        CompletionStage<ResponseFilterResult> filterResult = onMetadataResponse(metadataResponseData);
        assertThat(filterResult).isSameAs(FORWARD_FUTURE);
        verify(virtualClusterModel).rememberTopicIdMapping(TOPIC_UUID, TOPIC_NAME);
    }

    @Test
    void learnsTopicNameFromCreateTopicAndForwards() {
        when(filterContext.forwardResponse(any(), any())).thenReturn(FORWARD_FUTURE);
        CreateTopicsResponseData response = createTopicWithSingleTopic(t -> {
            t.setName(TOPIC_NAME);
            t.setTopicId(TOPIC_UUID);
        });
        CompletionStage<ResponseFilterResult> filterResult = onCreateTopicsResponse(response);
        assertThat(filterResult).isSameAs(FORWARD_FUTURE);
        verify(virtualClusterModel).rememberTopicIdMapping(TOPIC_UUID, TOPIC_NAME);
    }

    @Test
    void learnsAllTopicNameFromMetadataAndForwards() {
        when(filterContext.forwardResponse(any(), any())).thenReturn(FORWARD_FUTURE);
        MetadataResponseData response = new MetadataResponseData();
        MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic();
        topic.setName(TOPIC_NAME);
        topic.setTopicId(TOPIC_UUID);
        response.topics().add(topic);

        MetadataResponseData.MetadataResponseTopic topic2 = new MetadataResponseData.MetadataResponseTopic();
        String topicName2 = "topic2";
        Uuid topicId2 = Uuid.randomUuid();
        topic2.setName(topicName2);
        topic2.setTopicId(topicId2);
        response.topics().add(topic2);

        CompletionStage<ResponseFilterResult> response1 = onMetadataResponse(response);
        assertThat(response1).isSameAs(FORWARD_FUTURE);
        verify(virtualClusterModel).rememberTopicIdMapping(TOPIC_UUID, TOPIC_NAME);
        verify(virtualClusterModel).rememberTopicIdMapping(topicId2, topicName2);
    }

    @Test
    void learnsAllTopicNameFromCreateTopicAndForwards() {
        when(filterContext.forwardResponse(any(), any())).thenReturn(FORWARD_FUTURE);
        CreateTopicsResponseData response = new CreateTopicsResponseData();
        CreateTopicsResponseData.CreatableTopicResult topic = new CreateTopicsResponseData.CreatableTopicResult();
        topic.setName(TOPIC_NAME);
        topic.setTopicId(TOPIC_UUID);
        response.topics().add(topic);

        CreateTopicsResponseData.CreatableTopicResult topic2 = new CreateTopicsResponseData.CreatableTopicResult();
        String topicName2 = "topic2";
        Uuid topicId2 = Uuid.randomUuid();
        topic2.setName(topicName2);
        topic2.setTopicId(topicId2);
        response.topics().add(topic2);

        CompletionStage<ResponseFilterResult> filterResult = onCreateTopicsResponse(response);
        assertThat(filterResult).isSameAs(FORWARD_FUTURE);
        verify(virtualClusterModel).rememberTopicIdMapping(TOPIC_UUID, TOPIC_NAME);
        verify(virtualClusterModel).rememberTopicIdMapping(topicId2, topicName2);
    }

    private CompletionStage<ResponseFilterResult> onMetadataResponse(MetadataResponseData data) {
        return topicNameLearningFilter.onMetadataResponse((short) 12, new ResponseHeaderData(), data,
                filterContext);
    }

    private CompletionStage<ResponseFilterResult> onCreateTopicsResponse(CreateTopicsResponseData data) {
        return topicNameLearningFilter.onCreateTopicsResponse((short) 7, new ResponseHeaderData(), data,
                filterContext);
    }

    private static MetadataResponseData metadataWithSingleTopic(Consumer<MetadataResponseData.MetadataResponseTopic> topicModifier) {
        MetadataResponseData response = new MetadataResponseData();
        MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic();
        topicModifier.accept(topic);
        response.topics().add(topic);
        return response;
    }

    private static CreateTopicsResponseData createTopicWithSingleTopic(Consumer<CreateTopicsResponseData.CreatableTopicResult> topicModifier) {
        CreateTopicsResponseData response = new CreateTopicsResponseData();
        CreateTopicsResponseData.CreatableTopicResult topic = new CreateTopicsResponseData.CreatableTopicResult();
        topicModifier.accept(topic);
        response.topics().add(topic);
        return response;
    }

}