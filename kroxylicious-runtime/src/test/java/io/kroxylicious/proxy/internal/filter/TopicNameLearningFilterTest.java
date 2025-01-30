/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
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
        when(filterContext.forwardResponse(any(), any())).thenReturn(FORWARD_FUTURE);
    }

    static Stream<MetadataResponseData> learnsNothingFromMetadataAndForwards() {
        MetadataResponseData empty = new MetadataResponseData();
        MetadataResponseData nullTopic = new MetadataResponseData().setTopics(null);
        MetadataResponseData topicWithDefaultName = withSingleTopic(t -> {
            t.setTopicId(TOPIC_UUID);
        });
        MetadataResponseData topicWithNullName = withSingleTopic(t -> {
            t.setTopicId(TOPIC_UUID);
            t.setName(null);
        });
        MetadataResponseData topicWithDefaultTopicId = withSingleTopic(t -> {
            t.setName(TOPIC_NAME);
        });
        MetadataResponseData topicWithNullTopidId = withSingleTopic(t -> {
            t.setTopicId(null);
            t.setName(TOPIC_NAME);
        });
        return Stream.of(empty, nullTopic, topicWithDefaultName, topicWithNullName, topicWithDefaultTopicId, topicWithNullTopidId);
    }

    private static MetadataResponseData withSingleTopic(Consumer<MetadataResponseData.MetadataResponseTopic> topicModifier) {
        MetadataResponseData response = new MetadataResponseData();
        MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic();
        topicModifier.accept(topic);
        response.topics().add(topic);
        return response;
    }

    @ParameterizedTest
    @MethodSource
    void learnsNothingFromMetadataAndForwards(MetadataResponseData data) {
        CompletionStage<ResponseFilterResult> resultStage = onMetadataResponse(data);
        assertThat(resultStage).isSameAs(FORWARD_FUTURE);
        verify(virtualClusterModel, never()).rememberTopicIdMapping(any(), any());
    }

    @Test
    void learnsTopicName() {
        MetadataResponseData metadataResponseData = withSingleTopic(t -> {
            t.setName(TOPIC_NAME);
            t.setTopicId(TOPIC_UUID);
        });
        CompletionStage<ResponseFilterResult> response1 = onMetadataResponse(metadataResponseData);
        assertThat(response1).isSameAs(FORWARD_FUTURE);
        verify(virtualClusterModel).rememberTopicIdMapping(TOPIC_UUID, TOPIC_NAME);
    }

    @Test
    void learnsAllTopicNames() {
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

    private CompletionStage<ResponseFilterResult> onMetadataResponse(MetadataResponseData data) {
        return topicNameLearningFilter.onMetadataResponse((short) 12, new ResponseHeaderData(), data,
                filterContext);
    }

}