/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TopologyServiceImplTest {

    private static final String ROUTE = "route1";

    @Mock
    private RequestSender sender;

    private final TopologyCache cache = new TopologyCache();
    private TopologyServiceImpl topologyService;

    @BeforeEach
    void setUp() {
        topologyService = new TopologyServiceImpl(cache, sender);
    }

    private static MetadataResponseData metadataWithTopic(Uuid topicId, String topicName) {
        var response = new MetadataResponseData();
        response.topics().add(new MetadataResponseData.MetadataResponseTopic().setTopicId(topicId).setName(topicName));
        return response;
    }

    // --- topicNames ---

    @Test
    void topicNamesShouldReturnCacheHitsWithoutCallingSender() {
        // Given
        var topicId = Uuid.randomUuid();
        cache.updateFromMetadata(ROUTE, metadataWithTopic(topicId, "cached-topic"));

        // When
        var result = topologyService.topicNames(ROUTE, Set.of(topicId));

        // Then
        assertThat(result.toCompletableFuture()).isCompletedWithValue(Map.of(topicId, "cached-topic"));
        verifyNoInteractions(sender);
    }

    @Test
    void topicNamesShouldSendMetadataRequestForCacheMisses() {
        // Given
        var topicId = Uuid.randomUuid();
        when(sender.sendToAnyNode(eq(ROUTE), any(), any())).thenReturn(new CompletableFuture<>());

        // When
        topologyService.topicNames(ROUTE, Set.of(topicId));

        // Then
        var headerCaptor = ArgumentCaptor.forClass(RequestHeaderData.class);
        var requestCaptor = ArgumentCaptor.forClass(ApiMessage.class);
        verify(sender).sendToAnyNode(eq(ROUTE), headerCaptor.capture(), requestCaptor.capture());
        assertThat(headerCaptor.getValue().requestApiKey()).isEqualTo(ApiKeys.METADATA.id);
        assertThat(requestCaptor.getValue()).isInstanceOfSatisfying(MetadataRequestData.class, request -> {
            assertThat(request.allowAutoTopicCreation()).isFalse();
            assertThat(request.topics()).singleElement().satisfies(t -> assertThat(t.topicId()).isEqualTo(topicId));
        });
    }

    /**
     * The routing dispatcher populates the cache from the sender's response as a side effect
     * before the returned stage completes - simulated here by mutating the cache from within the
     * mocked sender's answer, exactly as the real dispatcher would.
     */
    @Test
    void topicNamesShouldReflectCachePopulationAfterSenderCompletes() {
        // Given
        var topicId = Uuid.randomUuid();
        when(sender.sendToAnyNode(eq(ROUTE), any(), any())).thenAnswer(invocation -> {
            cache.updateFromMetadata(ROUTE, metadataWithTopic(topicId, "resolved-topic"));
            return CompletableFuture.completedFuture(new MetadataResponseData());
        });

        // When
        var result = topologyService.topicNames(ROUTE, Set.of(topicId));

        // Then
        assertThat(result.toCompletableFuture()).isCompletedWithValue(Map.of(topicId, "resolved-topic"));
    }

    @Test
    void topicNamesShouldOmitIdsTheCacheNeverLearns() {
        // Given: the sender completes without the cache learning this id (e.g. a deleted topic)
        var topicId = Uuid.randomUuid();
        when(sender.sendToAnyNode(eq(ROUTE), any(), any())).thenReturn(CompletableFuture.completedFuture(new MetadataResponseData()));

        // When
        var result = topologyService.topicNames(ROUTE, Set.of(topicId));

        // Then
        assertThat(result.toCompletableFuture()).isCompletedWithValue(Map.of());
    }

    @Test
    void topicNamesShouldMergeCacheHitsWithNewlyResolvedIds() {
        // Given
        var cachedId = Uuid.randomUuid();
        var missingId = Uuid.randomUuid();
        cache.updateFromMetadata(ROUTE, metadataWithTopic(cachedId, "cached"));
        when(sender.sendToAnyNode(eq(ROUTE), any(), any())).thenAnswer(invocation -> {
            cache.updateFromMetadata(ROUTE, metadataWithTopic(missingId, "resolved"));
            return CompletableFuture.completedFuture(new MetadataResponseData());
        });

        // When
        var result = topologyService.topicNames(ROUTE, Set.of(cachedId, missingId));

        // Then
        assertThat(result.toCompletableFuture()).isCompletedWithValue(Map.of(cachedId, "cached", missingId, "resolved"));
        var requestCaptor = ArgumentCaptor.forClass(ApiMessage.class);
        verify(sender).sendToAnyNode(eq(ROUTE), any(), requestCaptor.capture());
        assertThat(requestCaptor.getValue()).isInstanceOfSatisfying(MetadataRequestData.class,
                request -> assertThat(request.topics()).singleElement().satisfies(t -> assertThat(t.topicId()).isEqualTo(missingId)));
    }

    // --- invalidateRoute ---

    @Test
    void invalidateRouteShouldDelegateToCache() {
        // Given
        var topicId = Uuid.randomUuid();
        cache.updateFromMetadata(ROUTE, metadataWithTopic(topicId, "topic"));

        // When
        topologyService.invalidateRoute(ROUTE);

        // Then
        assertThat(cache.topicName(ROUTE, topicId)).isEmpty();
    }

    // --- not yet implemented ---

    @Test
    void leadersShouldThrowUnsupportedOperationException() {
        assertThatThrownBy(() -> topologyService.leaders(Map.of())).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void coordinatorsShouldThrowUnsupportedOperationException() {
        assertThatThrownBy(() -> topologyService.coordinators(ROUTE, (byte) 0, Set.of())).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void partitionInfoShouldThrowUnsupportedOperationException() {
        assertThatThrownBy(() -> topologyService.partitionInfo("topic", 0)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void brokerInfoShouldThrowUnsupportedOperationException() {
        assertThatThrownBy(() -> topologyService.brokerInfo(null)).isInstanceOf(UnsupportedOperationException.class);
    }
}
