/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.netty.channel.EventLoop;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CachingTopicNameRetrieverTest {
    public static final Uuid UUID_2 = new Uuid(6L, 6L);
    private static final Uuid UUID = new Uuid(5L, 5L);
    public static final String TOPIC_NAME = "topicName";
    public static final String TOPIC_NAME_2 = "topicName2";
    public static final MapTopicNameMapping MAPPING = new MapTopicNameMapping(Map.of(UUID, TOPIC_NAME, UUID_2, TOPIC_NAME_2), Map.of());
    @Mock
    private EventLoop eventExecutor;

    @Mock
    private FilterContext filterContext;

    @Mock
    TopicNameRetriever delegate;

    TopicNameRetriever retriever;

    @BeforeEach
    public void setUp() {
        retriever = CachingTopicNameRetriever.cachingRetriever(delegate);
    }

    @Test
    public void topicNameRetrieverEmptyRequest() {
        // when
        CompletionStage<TopicNameMapping> result = retriever.topicNames(Set.of(), eventExecutor, filterContext);
        // then
        assertThat(result).isInstanceOfSatisfying(InternalCompletableFuture.class, f -> {
            assertThat(f.defaultExecutor()).isSameAs(eventExecutor);
        }).succeedsWithin(0, TimeUnit.SECONDS).isEqualTo(MapTopicNameMapping.EMPTY);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void topicNameRetrieverDelegates() {
        // given
        CompletableFuture<TopicNameMapping> future = CompletableFuture.completedFuture(MAPPING);
        when(delegate.topicNames(Set.of(UUID, UUID_2), eventExecutor, filterContext)).thenReturn(future);
        // when
        CompletionStage<TopicNameMapping> result = retriever.topicNames(Set.of(UUID, UUID_2), eventExecutor, filterContext);
        // then
        assertThat(result).succeedsWithin(0, TimeUnit.SECONDS).isEqualTo(MAPPING);
    }

    @Test
    public void topicNameRetrieverCaches() {
        // given
        CompletableFuture<TopicNameMapping> future = CompletableFuture.completedFuture(MAPPING);
        when(delegate.topicNames(Set.of(UUID, UUID_2), eventExecutor, filterContext)).thenReturn(future);
        retriever.topicNames(Set.of(UUID, UUID_2), eventExecutor, filterContext);
        verify(delegate).topicNames(Set.of(UUID, UUID_2), eventExecutor, filterContext);
        // when
        CompletionStage<TopicNameMapping> result = retriever.topicNames(Set.of(UUID, UUID_2), eventExecutor, filterContext);
        // then
        assertThat(result.getClass()).isAssignableTo(InternalCompletionStage.class);
        // assertThat converts the internal minimal stage to future
        assertThat(result).isInstanceOfSatisfying(InternalCompletableFuture.class, f -> {
            assertThat(f.defaultExecutor()).isSameAs(eventExecutor);
        }).succeedsWithin(0, TimeUnit.SECONDS).isEqualTo(MAPPING);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void topicNameRetrieverRequestPartiallyCached() {
        // given
        MapTopicNameMapping expected = new MapTopicNameMapping(Map.of(UUID, TOPIC_NAME), Map.of());
        CompletableFuture<TopicNameMapping> future = CompletableFuture.completedFuture(expected);
        when(delegate.topicNames(Set.of(UUID), eventExecutor, filterContext)).thenReturn(future);
        retriever.topicNames(Set.of(UUID), eventExecutor, filterContext);
        verify(delegate).topicNames(Set.of(UUID), eventExecutor, filterContext);
        // when
        CompletableFuture<TopicNameMapping> future2 = CompletableFuture.completedFuture(MAPPING);
        when(delegate.topicNames(Set.of(UUID, UUID_2), eventExecutor, filterContext)).thenReturn(future2);
        CompletionStage<TopicNameMapping> result = retriever.topicNames(Set.of(UUID, UUID_2), eventExecutor, filterContext);
        // then
        assertThat(result).succeedsWithin(0, TimeUnit.SECONDS).isEqualTo(MAPPING);
        verify(delegate).topicNames(Set.of(UUID, UUID_2), eventExecutor, filterContext);
        verifyNoMoreInteractions(delegate);
    }

}