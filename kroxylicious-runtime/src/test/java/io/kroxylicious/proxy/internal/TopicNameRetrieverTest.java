/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.TopicNameLookupException;
import io.kroxylicious.proxy.filter.TopicNameResult;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TopicNameRetrieverTest {

    public static final Uuid UUID_2 = new Uuid(6L, 6L);
    private static final Uuid UUID = new Uuid(5L, 5L);
    public static final String TOPIC_NAME = "topicName";
    public static final String TOPIC_NAME_2 = "topicName2";

    @Mock
    private FilterContext filterContext;
    private TopicNameRetriever retriever;

    @BeforeEach
    public void setUp() {
        retriever = new TopicNameRetriever(filterContext);
    }

    @Test
    void retrieveTopicName() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        response.topics().add(getResponseTopic(UUID, TOPIC_NAME));
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<Map<Uuid, TopicNameResult>> topicNames = getTopicNames(Set.of(UUID));
        // then
        assertOnlyTopicNameResultSatisfies(topicNames, UUID, topicNameResult -> {
            assertThat(topicNameResult.exception()).isNull();
            assertThat(topicNameResult.topicName()).isEqualTo(TOPIC_NAME);
        });
    }

    @Test
    void retrieveTopicNames() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        response.topics().add(getResponseTopic(UUID, TOPIC_NAME));
        response.topics().add(getResponseTopic(UUID_2, TOPIC_NAME_2));
        Set<Uuid> topicIds = Set.of(UUID, UUID_2);
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<Map<Uuid, TopicNameResult>> topicNames = getTopicNames(topicIds);
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesResult -> {
                    assertThat(topicNamesResult).containsOnlyKeys(UUID, UUID_2);
                    assertThat(topicNamesResult).hasEntrySatisfying(UUID, topicNameResult -> {
                        assertThat(topicNameResult.exception()).isNull();
                        assertThat(topicNameResult.topicName()).isEqualTo(TOPIC_NAME);
                    });
                    assertThat(topicNamesResult).hasEntrySatisfying(UUID_2, topicNameResult -> {
                        assertThat(topicNameResult.exception()).isNull();
                        assertThat(topicNameResult.topicName()).isEqualTo(TOPIC_NAME_2);
                    });
                });
    }

    @Test
    void retrieveTopicNamesServerPartialResposne() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        response.topics().add(getResponseTopic(UUID, TOPIC_NAME));
        Set<Uuid> topicIds = Set.of(UUID, UUID_2);
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<Map<Uuid, TopicNameResult>> topicNames = getTopicNames(topicIds);
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesResult -> {
                    assertThat(topicNamesResult).containsOnlyKeys(UUID, UUID_2);
                    assertThat(topicNamesResult).hasEntrySatisfying(UUID, topicNameResult -> {
                        assertThat(topicNameResult.exception()).isNull();
                        assertThat(topicNameResult.topicName()).isEqualTo(TOPIC_NAME);
                    });
                    assertThat(topicNamesResult).hasEntrySatisfying(UUID_2, topicNameResult -> {
                        assertThat(topicNameResult.topicName()).isNull();
                        assertThat(topicNameResult.exception()).isNotNull().isInstanceOf(TopicNameLookupException.class)
                                .hasMessageContaining("Topic not found in Metadata response");
                    });
                });
    }

    @Test
    void retrieveTopicNamesHandlesSendFutureFailing() {
        // given
        givenSendRequestResponse(failedFuture(new RuntimeException("BOOM")));
        // when
        CompletionStage<Map<Uuid, TopicNameResult>> topicNames = getTopicNames(Set.of(UUID));
        // then
        assertOnlyTopicNameResultSatisfies(topicNames, UUID, topicNameResult -> {
            assertThat(topicNameResult.topicName()).isNull();
            assertThat(topicNameResult.exception())
                    .hasMessageContaining("get topic names failed unexpectedly");
        });
    }

    @Test
    void unexpectedResponseType() {
        // given
        ApiMessage response = new ApiVersionsResponseData();
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<Map<Uuid, TopicNameResult>> topicNames = getTopicNames(Set.of(UUID));
        // then
        assertOnlyTopicNameResultSatisfies(topicNames, UUID, topicNameResult -> {
            assertThat(topicNameResult.topicName()).isNull();
            assertThat(topicNameResult.exception()).isNotNull().isInstanceOf(TopicNameLookupException.class)
                    .hasMessageContaining("Unexpected response type class org.apache.kafka.common.message.ApiVersionsResponseData");
        });
    }

    @Test
    void emptyResponse() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<Map<Uuid, TopicNameResult>> topicNames = getTopicNames(Set.of(UUID));
        // then
        assertOnlyTopicNameResultSatisfies(topicNames, UUID, topicNameResult -> {
            assertThat(topicNameResult.topicName()).isNull();
            assertThat(topicNameResult.exception()).isNotNull().isInstanceOf(TopicNameLookupException.class)
                    .hasMessageContaining("Topic not found in Metadata response");
        });
    }

    @Test
    void topLevelResponseError() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        response.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<Map<Uuid, TopicNameResult>> topicNames = getTopicNames(Set.of(UUID));
        // then
        assertOnlyTopicNameResultSatisfies(topicNames, UUID, topicNameResult -> {
            assertThat(topicNameResult.topicName()).isNull();
            assertThat(topicNameResult.exception()).isNotNull().isInstanceOf(TopicNameLookupException.class)
                    .hasMessageContaining(
                            "MetadataResponse top level error: UNKNOWN_SERVER_ERROR, The server experienced an unexpected error when processing the request.");
        });
    }

    @Test
    void topicLevelError() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        MetadataResponseData.MetadataResponseTopic responseTopic = new MetadataResponseData.MetadataResponseTopic().setTopicId(UUID)
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
        response.topics().add(responseTopic);
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<Map<Uuid, TopicNameResult>> topicNames = getTopicNames(Set.of(UUID));
        // then
        assertOnlyTopicNameResultSatisfies(topicNames, UUID, topicNameResult -> {
            assertThat(topicNameResult.topicName()).isNull();
            assertThat(topicNameResult.exception()).isNotNull().isInstanceOf(TopicNameLookupException.class)
                    .hasMessageContaining(
                            "topic level error: UNKNOWN_SERVER_ERROR, The server experienced an unexpected error when processing the request.");
        });
    }

    private static MetadataResponseData.MetadataResponseTopic getResponseTopic(Uuid uuid, String topicName) {
        return new MetadataResponseData.MetadataResponseTopic()
                .setTopicId(uuid)
                .setName(topicName);
    }

    private static void assertOnlyTopicNameResultSatisfies(CompletionStage<Map<Uuid, TopicNameResult>> topicNames, Uuid uuid,
                                                           Consumer<TopicNameResult> topicNameResultConsumer) {
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesResult -> {
                    assertThat(topicNamesResult).containsOnlyKeys(uuid);
                    assertThat(topicNamesResult).hasEntrySatisfying(uuid, topicNameResultConsumer);
                });
    }

    private void givenSendRequestResponse(CompletableFuture<ApiMessage> response) {
        when(filterContext.sendRequest(any(), any())).thenReturn(response);
    }

    private CompletionStage<Map<Uuid, TopicNameResult>> getTopicNames(Set<Uuid> topicIds) {
        return retriever.getTopicNames(topicIds);
    }

}