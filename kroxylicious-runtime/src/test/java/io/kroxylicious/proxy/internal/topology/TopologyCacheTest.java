/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.topology;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.message.MetadataResponseData;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class TopologyCacheTest {

    private static final String ROUTE = "route1";
    private static final String OTHER_ROUTE = "route2";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final String TOPIC_NAME = "topicName";

    private final TopologyCache cache = new TopologyCache();

    private static MetadataResponseData metadataWithTopic(@Nullable Uuid topicId, @Nullable String topicName) {
        var response = new MetadataResponseData();
        var topic = new MetadataResponseData.MetadataResponseTopic();
        topic.setTopicId(topicId);
        topic.setName(topicName);
        response.topics().add(topic);
        return response;
    }

    @Test
    void updateFromMetadataShouldPopulateTopicName() {
        // Given
        var response = metadataWithTopic(TOPIC_ID, TOPIC_NAME);

        // When
        cache.updateFromMetadata(ROUTE, response);

        // Then
        assertThat(cache.topicName(ROUTE, TOPIC_ID)).contains(TOPIC_NAME);
    }

    @Test
    void updateFromMetadataShouldIgnoreResponseWithNullTopics() {
        // Given
        var response = new MetadataResponseData();
        response.setTopics(null);

        // When / Then
        assertThatCode(() -> cache.updateFromMetadata(ROUTE, response)).doesNotThrowAnyException();
    }

    static Stream<Arguments> unlearnableTopics() {
        return Stream.of(
                Arguments.argumentSet("null topic name", TOPIC_ID, null),
                Arguments.argumentSet("empty topic name", TOPIC_ID, ""),
                Arguments.argumentSet("zero topic id", Uuid.ZERO_UUID, TOPIC_NAME),
                Arguments.argumentSet("zero topic id (distinct instance)", new Uuid(0L, 0L), TOPIC_NAME),
                Arguments.argumentSet("null topic id", null, TOPIC_NAME));
    }

    @ParameterizedTest
    @MethodSource("unlearnableTopics")
    void updateFromMetadataShouldNotCacheUnlearnableTopics(@Nullable Uuid topicId, @Nullable String topicName) {
        // Given
        var response = metadataWithTopic(topicId, topicName);

        // When
        cache.updateFromMetadata(ROUTE, response);

        // Then
        assertThat(cache.topicName(ROUTE, TOPIC_ID)).isEmpty();
    }

    @Test
    void topicNameShouldBeScopedByRoute() {
        // Given
        cache.updateFromMetadata(ROUTE, metadataWithTopic(TOPIC_ID, "route1-name"));
        cache.updateFromMetadata(OTHER_ROUTE, metadataWithTopic(TOPIC_ID, "route2-name"));

        // When / Then
        assertThat(cache.topicName(ROUTE, TOPIC_ID)).contains("route1-name");
        assertThat(cache.topicName(OTHER_ROUTE, TOPIC_ID)).contains("route2-name");
    }

    @Test
    void topicNameShouldBeEmptyForUncachedRoute() {
        // When / Then
        assertThat(cache.topicName(ROUTE, TOPIC_ID)).isEmpty();
    }

    /**
     * A response that omits a previously-learned topic must not evict it - a router only sees
     * the topics it happened to ask about or that were included in whatever METADATA response
     * flowed through, not a complete cluster snapshot.
     */
    @Test
    void updateFromMetadataShouldBeAdditive() {
        // Given
        var firstTopicId = Uuid.randomUuid();
        var secondTopicId = Uuid.randomUuid();
        cache.updateFromMetadata(ROUTE, metadataWithTopic(firstTopicId, "first"));

        // When
        cache.updateFromMetadata(ROUTE, metadataWithTopic(secondTopicId, "second"));

        // Then
        assertThat(cache.topicName(ROUTE, firstTopicId)).contains("first");
        assertThat(cache.topicName(ROUTE, secondTopicId)).contains("second");
    }

    @Test
    void updateFromMetadataShouldOverwriteExistingEntry() {
        // Given
        cache.updateFromMetadata(ROUTE, metadataWithTopic(TOPIC_ID, "old-name"));

        // When
        cache.updateFromMetadata(ROUTE, metadataWithTopic(TOPIC_ID, "new-name"));

        // Then
        assertThat(cache.topicName(ROUTE, TOPIC_ID)).contains("new-name");
    }

    @Test
    void invalidateRouteShouldClearOnlyThatRoute() {
        // Given
        cache.updateFromMetadata(ROUTE, metadataWithTopic(TOPIC_ID, TOPIC_NAME));
        cache.updateFromMetadata(OTHER_ROUTE, metadataWithTopic(TOPIC_ID, TOPIC_NAME));

        // When
        cache.invalidateRoute(ROUTE);

        // Then
        assertThat(cache.topicName(ROUTE, TOPIC_ID)).isEmpty();
        assertThat(cache.topicName(OTHER_ROUTE, TOPIC_ID)).contains(TOPIC_NAME);
    }

    @Test
    void invalidateRouteShouldBeSafeForUncachedRoute() {
        // When / Then
        assertThatCode(() -> cache.invalidateRoute(ROUTE)).doesNotThrowAnyException();
    }
}
