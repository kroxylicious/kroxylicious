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
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownServerException;
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
import io.kroxylicious.proxy.filter.TopicNameMapping;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
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
        CompletionStage<TopicNameMapping> topicNames = getTopicNames(Set.of(UUID));
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesMapping -> {
                    assertThat(topicNamesMapping.anyFailures()).isFalse();
                    assertThat(topicNamesMapping.topicNames()).containsExactly(entry(UUID, TOPIC_NAME));
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
        CompletionStage<TopicNameMapping> topicNames = getTopicNames(topicIds);
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNameMapping -> {
                    assertThat(topicNameMapping.anyFailures()).isFalse();
                    Map<Uuid, String> uuidTopicNameResultMap = topicNameMapping.topicNames();
                    assertThat(uuidTopicNameResultMap).containsExactlyInAnyOrderEntriesOf(Map.of(UUID, TOPIC_NAME, UUID_2, TOPIC_NAME_2));
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
        CompletionStage<TopicNameMapping> topicNames = getTopicNames(topicIds);
        // then
        assertThat(topicNames.toCompletableFuture()).failsWithin(Duration.ZERO)
                .withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(TopicNameLookupException.class)
                .withMessage("Not all requested uuids present in Metadata, missing uuids: [" + UUID_2 + "]");
    }

    @Test
    void retrieveTopicNamesHandlesSendFutureFailing() {
        // given
        RuntimeException exception = new RuntimeException("BOOM");
        givenSendRequestResponse(failedFuture(exception));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNames(Set.of(UUID));
        // then
        assertThat(topicNames.toCompletableFuture()).failsWithin(Duration.ZERO)
                .withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(TopicNameLookupException.class)
                .withMessage("getTopicNames resulted in unhandled exception")
                .havingCause().isInstanceOf(CompletionException.class)
                .withCause(exception);
    }

    @Test
    void unexpectedResponseType() {
        // given
        ApiMessage response = new ApiVersionsResponseData();
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNames(Set.of(UUID));
        // then
        assertThat(topicNames.toCompletableFuture()).failsWithin(Duration.ZERO)
                .withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(TopicNameLookupException.class)
                .withMessage("unexpected response type: ApiVersionsResponseData");
    }

    @Test
    void emptyResponse() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNames(Set.of(UUID));
        // then
        assertThat(topicNames.toCompletableFuture()).failsWithin(Duration.ZERO)
                .withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(TopicNameLookupException.class)
                .withMessage("Not all requested uuids present in Metadata, missing uuids: [" + UUID + "]");
    }

    @Test
    void topLevelResponseError() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        response.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNames(Set.of(UUID));
        // then
        assertThat(topicNames.toCompletableFuture()).failsWithin(Duration.ZERO)
                .withThrowableThat().isInstanceOf(ExecutionException.class)
                .havingCause().isInstanceOf(TopicNameLookupException.class)
                .withMessage("getTopicNames Metadata response contained a top level Error code: UNKNOWN_SERVER_ERROR")
                .havingCause().isInstanceOf(UnknownServerException.class);
    }

    @Test
    void topicLevelError() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        MetadataResponseData.MetadataResponseTopic responseTopic = new MetadataResponseData.MetadataResponseTopic().setTopicId(UUID)
                .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code());
        response.topics().add(responseTopic);
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNames(Set.of(UUID));
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesMapping -> {
                    assertThat(topicNamesMapping.anyFailures()).isTrue();
                    assertThat(topicNamesMapping.failures()).containsExactly(entry(UUID, Errors.UNKNOWN_TOPIC_ID));
                });
    }

    @Test
    void mixtureOfSuccessAndTopicLevelFailure() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        MetadataResponseData.MetadataResponseTopic responseTopic = new MetadataResponseData.MetadataResponseTopic().setTopicId(UUID)
                .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code());
        MetadataResponseData.MetadataResponseTopic responseTopic2 = new MetadataResponseData.MetadataResponseTopic().setTopicId(UUID_2)
                .setName(TOPIC_NAME_2);
        response.topics().add(responseTopic);
        response.topics().add(responseTopic2);
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNames(Set.of(UUID, UUID_2));
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesMapping -> {
                    assertThat(topicNamesMapping.anyFailures()).isTrue();
                    assertThat(topicNamesMapping.topicNames()).containsExactly(entry(UUID_2, TOPIC_NAME_2));
                    assertThat(topicNamesMapping.failures()).containsExactly(entry(UUID, Errors.UNKNOWN_TOPIC_ID));
                });
    }

    private static MetadataResponseData.MetadataResponseTopic getResponseTopic(Uuid uuid, String topicName) {
        return new MetadataResponseData.MetadataResponseTopic()
                .setTopicId(uuid)
                .setName(topicName);
    }

    private void givenSendRequestResponse(CompletableFuture<ApiMessage> response) {
        when(filterContext.sendRequest(any(), any())).thenReturn(response);
    }

    private CompletionStage<TopicNameMapping> getTopicNames(Set<Uuid> topicIds) {
        return retriever.getTopicNames(topicIds);
    }

}
