/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.internal.util.RequestHeaderTagger;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.config.CacheConfiguration.DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TopicNameCacheFilterTest {

    public static final String TOPIC_NAME = "topicName";
    public static final Uuid TOPIC_ID = Uuid.randomUuid();
    @Mock
    private FilterContext filterContext;
    @Mock
    private RequestFilterResultBuilder requestFilterResultBuilder;
    @Mock
    private CloseOrTerminalStage<RequestFilterResult> closeOrTerminalStage;

    private static final String CLUSTER_NAME = "clusterName";

    @Test
    void onMetadataRequestWithoutTag() {
        // given
        TopicNameCacheFilter topicNameCacheFilter = new TopicNameCacheFilter(DEFAULT, Map.of(TOPIC_ID, TOPIC_NAME), CLUSTER_NAME);
        RequestHeaderData header = new RequestHeaderData();
        MetadataRequestData request = new MetadataRequestData();
        MetadataRequestTopic topic = new MetadataRequestTopic();
        topic.setTopicId(TOPIC_ID);
        request.topics().add(topic);
        CompletableFuture<RequestFilterResult> result = CompletableFuture.completedFuture(null);
        when(filterContext.forwardRequest(header, request)).thenReturn(result);
        // when
        CompletionStage<RequestFilterResult> resultStage = topicNameCacheFilter.onMetadataRequest(ApiKeys.METADATA.latestVersion(), header,
                request, filterContext);
        // then
        // request is forwarded upstream, we do not shortcircuit unless a specific tag is present on the request header
        assertThat(resultStage).isSameAs(result);
    }

    @Test
    void onMetadataRequestWithTagTopicsEmpty() {
        // given
        TopicNameCacheFilter topicNameCacheFilter = new TopicNameCacheFilter(DEFAULT, Map.of(TOPIC_ID, TOPIC_NAME), CLUSTER_NAME);
        RequestHeaderData header = new RequestHeaderData();
        RequestHeaderTagger.tag(header, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        MetadataRequestData request = new MetadataRequestData();
        CompletableFuture<RequestFilterResult> result = CompletableFuture.completedFuture(null);
        when(filterContext.requestFilterResultBuilder()).thenReturn(requestFilterResultBuilder);
        when(requestFilterResultBuilder.errorResponse(Mockito.eq(header), Mockito.eq(request), Mockito.any())).thenReturn(closeOrTerminalStage);
        when(closeOrTerminalStage.completed()).thenReturn(result);
        // when
        CompletionStage<RequestFilterResult> resultStage = topicNameCacheFilter.onMetadataRequest(ApiKeys.METADATA.latestVersion(), header,
                request, filterContext);
        // then
        // we respond with an error as it's an illegal state for an internal topic name retrieval request to have an empty topics list
        assertThat(resultStage).isSameAs(result);
        ArgumentCaptor<ApiException> captor = ArgumentCaptor.forClass(ApiException.class);
        Mockito.verify(requestFilterResultBuilder).errorResponse(Mockito.eq(header), Mockito.eq(request), captor.capture());
        assertThat(captor.getValue()).isInstanceOf(UnknownServerException.class).hasMessage("received an internal topic name request with no topics");
    }

    @Test
    void onMetadataRequestWithTagTopicsNull() {
        // given
        TopicNameCacheFilter topicNameCacheFilter = new TopicNameCacheFilter(DEFAULT, Map.of(TOPIC_ID, TOPIC_NAME), CLUSTER_NAME);
        RequestHeaderData header = new RequestHeaderData();
        RequestHeaderTagger.tag(header, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        MetadataRequestData request = new MetadataRequestData();
        request.setTopics(null);
        CompletableFuture<RequestFilterResult> result = CompletableFuture.completedFuture(null);
        when(filterContext.requestFilterResultBuilder()).thenReturn(requestFilterResultBuilder);
        when(requestFilterResultBuilder.errorResponse(Mockito.eq(header), Mockito.eq(request), Mockito.any())).thenReturn(closeOrTerminalStage);
        when(closeOrTerminalStage.completed()).thenReturn(result);
        // when
        CompletionStage<RequestFilterResult> resultStage = topicNameCacheFilter.onMetadataRequest(ApiKeys.METADATA.latestVersion(), header,
                request, filterContext);
        // then
        // we respond with an error as it's an illegal state for an internal topic name retrieval request to have an empty topics list
        assertThat(resultStage).isSameAs(result);
        ArgumentCaptor<ApiException> captor = ArgumentCaptor.forClass(ApiException.class);
        Mockito.verify(requestFilterResultBuilder).errorResponse(Mockito.eq(header), Mockito.eq(request), captor.capture());
        assertThat(captor.getValue()).isInstanceOf(UnknownServerException.class).hasMessage("received an internal topic name request with no topics");
    }

    @Test
    void onMetadataRequestWithTagAndAllTopicIdsCached() {
        // given
        TopicNameCacheFilter topicNameCacheFilter = new TopicNameCacheFilter(DEFAULT, Map.of(TOPIC_ID, TOPIC_NAME), CLUSTER_NAME);
        RequestHeaderData header = new RequestHeaderData();
        RequestHeaderTagger.tag(header, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        MetadataRequestData request = new MetadataRequestData();
        MetadataRequestTopic topic = new MetadataRequestTopic();
        topic.setTopicId(TOPIC_ID);
        request.topics().add(topic);
        CompletableFuture<RequestFilterResult> result = CompletableFuture.completedFuture(null);
        when(filterContext.requestFilterResultBuilder()).thenReturn(requestFilterResultBuilder);
        when(requestFilterResultBuilder.shortCircuitResponse(Mockito.any())).thenReturn(closeOrTerminalStage);
        when(closeOrTerminalStage.completed()).thenReturn(result);
        // when
        CompletionStage<RequestFilterResult> resultStage = topicNameCacheFilter.onMetadataRequest(ApiKeys.METADATA.latestVersion(), header,
                request, filterContext);
        // then
        assertThat(resultStage).isSameAs(result);
        ArgumentCaptor<ApiMessage> captor = ArgumentCaptor.forClass(ApiMessage.class);
        Mockito.verify(requestFilterResultBuilder).shortCircuitResponse(captor.capture());
        ApiMessage value = captor.getValue();
        assertThat(value).isInstanceOfSatisfying(MetadataResponseData.class, metadataResponseData -> {
            assertThat(metadataResponseData.topics()).isNotNull().singleElement().satisfies(metadataResponseTopic -> {
                assertThat(metadataResponseTopic.topicId()).isEqualTo(TOPIC_ID);
                assertThat(metadataResponseTopic.name()).isEqualTo(TOPIC_NAME);
            });
        });
    }

    @Test
    void onMetadataRequestWithTagAndTopicIdsNotCached() {
        // given
        TopicNameCacheFilter topicNameCacheFilter = new TopicNameCacheFilter(DEFAULT, Map.of(), CLUSTER_NAME);
        RequestHeaderData header = new RequestHeaderData();
        RequestHeaderTagger.tag(header, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        MetadataRequestData request = new MetadataRequestData();
        MetadataRequestTopic topic = new MetadataRequestTopic();
        topic.setTopicId(TOPIC_ID);
        request.topics().add(topic);
        CompletableFuture<RequestFilterResult> result = CompletableFuture.completedFuture(null);
        when(filterContext.forwardRequest(Mockito.any(), Mockito.any())).thenReturn(result);
        // when
        CompletionStage<RequestFilterResult> resultStage = topicNameCacheFilter.onMetadataRequest(ApiKeys.METADATA.latestVersion(), header,
                request, filterContext);
        // then
        assertThat(resultStage).isSameAs(result);
        ArgumentCaptor<RequestHeaderData> headerCaptor = ArgumentCaptor.forClass(RequestHeaderData.class);
        ArgumentCaptor<ApiMessage> captor = ArgumentCaptor.forClass(ApiMessage.class);
        Mockito.verify(filterContext).forwardRequest(headerCaptor.capture(), captor.capture());
        ApiMessage value = captor.getValue();
        assertThat(value).isInstanceOfSatisfying(MetadataRequestData.class, requestData -> {
            assertThat(requestData).isSameAs(request);
        });
        RequestHeaderData sendHeader = headerCaptor.getValue();
        assertThat(header).isSameAs(sendHeader);
        // tag removed before forwarding upstream
        assertThat(RequestHeaderTagger.containsTag(sendHeader, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES)).isFalse();
    }

    @Test
    void onMetadataResponseWithNoTopics() {
        // given
        TopicNameCacheFilter topicNameCacheFilter = new TopicNameCacheFilter(DEFAULT, CLUSTER_NAME);
        ResponseHeaderData header = new ResponseHeaderData();
        MetadataResponseData response = new MetadataResponseData();
        CompletableFuture<ResponseFilterResult> result = CompletableFuture.completedFuture(null);
        when(filterContext.forwardResponse(header, response)).thenReturn(result);
        // when
        CompletionStage<ResponseFilterResult> responseFilterResultCompletionStage = topicNameCacheFilter.onMetadataResponse(ApiKeys.METADATA.latestVersion(), header,
                response, filterContext);
        // then
        assertThat(responseFilterResultCompletionStage).isSameAs(result);
    }

    @Test
    void onMetadataResponseWithTopics() {
        // given
        TopicNameCacheFilter topicNameCacheFilter = new TopicNameCacheFilter(DEFAULT, CLUSTER_NAME);
        ResponseHeaderData header = new ResponseHeaderData();
        MetadataResponseData response = new MetadataResponseData();
        MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic();
        topic.setTopicId(TOPIC_ID);
        topic.setName(TOPIC_NAME);
        response.topics().add(topic);
        CompletableFuture<ResponseFilterResult> result = CompletableFuture.completedFuture(null);
        when(filterContext.forwardResponse(header, response)).thenReturn(result);
        // when
        CompletionStage<ResponseFilterResult> responseFilterResultCompletionStage = topicNameCacheFilter.onMetadataResponse(ApiKeys.METADATA.latestVersion(), header,
                response, filterContext);
        // then
        assertThat(responseFilterResultCompletionStage).isSameAs(result);
        assertThat(topicNameCacheFilter.topicName(TOPIC_ID)).contains(TOPIC_NAME);
    }

    public static Stream<Arguments> onMetadataResponseWithUnlearnableTopic() {
        return Stream.of(Arguments.argumentSet("null topic name", TOPIC_ID, null),
                Arguments.argumentSet("empty topic name", TOPIC_ID, ""),
                Arguments.argumentSet("zero topic id", Uuid.ZERO_UUID, TOPIC_NAME),
                Arguments.argumentSet("null topic id", null, TOPIC_NAME));
    }

    @Test
    void onMetadataResponseWithNullTopics() {
        // given
        TopicNameCacheFilter topicNameCacheFilter = new TopicNameCacheFilter(DEFAULT, CLUSTER_NAME);
        ResponseHeaderData header = new ResponseHeaderData();
        MetadataResponseData response = new MetadataResponseData();
        response.setTopics(null);
        CompletableFuture<ResponseFilterResult> result = CompletableFuture.completedFuture(null);
        when(filterContext.forwardResponse(header, response)).thenReturn(result);
        // when
        CompletionStage<ResponseFilterResult> responseFilterResultCompletionStage = topicNameCacheFilter.onMetadataResponse(ApiKeys.METADATA.latestVersion(), header,
                response, filterContext);
        // then
        assertThat(responseFilterResultCompletionStage).isSameAs(result);
        assertThat(topicNameCacheFilter.topicName(TOPIC_ID)).isEmpty();
    }

    @ParameterizedTest
    @MethodSource
    void onMetadataResponseWithUnlearnableTopic(@Nullable Uuid topicId, @Nullable String topicName) {
        // given
        TopicNameCacheFilter topicNameCacheFilter = new TopicNameCacheFilter(DEFAULT, CLUSTER_NAME);
        ResponseHeaderData header = new ResponseHeaderData();
        MetadataResponseData response = new MetadataResponseData();
        MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic();
        topic.setTopicId(topicId);
        topic.setName(topicName);
        response.topics().add(topic);
        CompletableFuture<ResponseFilterResult> result = CompletableFuture.completedFuture(null);
        when(filterContext.forwardResponse(header, response)).thenReturn(result);
        // when
        CompletionStage<ResponseFilterResult> responseFilterResultCompletionStage = topicNameCacheFilter.onMetadataResponse(ApiKeys.METADATA.latestVersion(), header,
                response, filterContext);
        // then
        assertThat(responseFilterResultCompletionStage).isSameAs(result);
        assertThat(topicNameCacheFilter.topicName(TOPIC_ID)).isEmpty();
    }

    @Test
    void defaultCacheConfig() {
        // given
        CacheConfiguration cacheConfig = DEFAULT;
        // when
        TopicNameCacheFilter filter = new TopicNameCacheFilter(cacheConfig, CLUSTER_NAME);
        // then
        assertThat(filter.topicNameCache.policy().eviction()).isEmpty();
        assertThat(filter.topicNameCache.policy().expireAfterAccess()).hasValueSatisfying(expiratioon -> {
            assertThat(expiratioon.getExpiresAfter()).isEqualTo(Duration.ofHours(1));
        });
        assertThat(filter.topicNameCache.policy().expireAfterWrite()).isEmpty();
    }

    @Test
    void cacheStatsEnabled() {
        // when
        TopicNameCacheFilter filter = new TopicNameCacheFilter(DEFAULT, CLUSTER_NAME);
        // then
        assertThat(filter.topicNameCache.policy().isRecordingStats()).isTrue();
    }

    @Test
    void expiryConfig() {
        // given
        CacheConfiguration cacheConfig = new CacheConfiguration(null, Duration.of(10L, ChronoUnit.SECONDS), Duration.of(10L, ChronoUnit.SECONDS));
        // when
        TopicNameCacheFilter filter = new TopicNameCacheFilter(cacheConfig, CLUSTER_NAME);
        // then
        assertThat(filter.topicNameCache.policy().expireAfterAccess()).hasValueSatisfying(expiratioon -> {
            assertThat(expiratioon.getExpiresAfter()).isEqualTo(Duration.ofSeconds(10));
        });
        assertThat(filter.topicNameCache.policy().expireAfterWrite()).hasValueSatisfying(expiration -> {
            assertThat(expiration.getExpiresAfter()).isEqualTo(Duration.ofSeconds(10));
        });
    }

    @Test
    void maxSizeConfig() {
        // given
        CacheConfiguration cacheConfig = new CacheConfiguration(5, null, null);
        // when
        TopicNameCacheFilter filter = new TopicNameCacheFilter(cacheConfig, CLUSTER_NAME);
        // then
        assertThat(filter.topicNameCache.policy().eviction()).hasValueSatisfying(eviction -> {
            assertThat(eviction.getMaximum()).isEqualTo(5L);
        });
    }
}
