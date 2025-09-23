/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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
import io.kroxylicious.proxy.filter.metadata.TopLevelMetadataErrorException;
import io.kroxylicious.proxy.filter.metadata.TopicLevelMetadataErrorException;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;

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
    void setUp() {
        retriever = new TopicNameRetriever(filterContext);
    }

    @Test
    void retrieveTopicNamesMapping() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        response.topics().add(getResponseTopic(UUID, TOPIC_NAME));
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNamesMapping(Set.of(UUID));
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesMapping -> {
                    assertThat(topicNamesMapping.anyFailures()).isFalse();
                    assertThat(topicNamesMapping.topicNames()).containsExactly(entry(UUID, TOPIC_NAME));
                });
    }

    @Test
    void retrieveTopicNamesMappingServerPartialResponse() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        response.topics().add(getResponseTopic(UUID, TOPIC_NAME));
        Set<Uuid> topicIds = Set.of(UUID, UUID_2);
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNamesMapping(topicIds);
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesMapping -> {
                    assertThat(topicNamesMapping.anyFailures()).isTrue();
                    assertThat(topicNamesMapping.failures()).containsOnlyKeys(UUID_2)
                            .hasEntrySatisfying(UUID_2, errors -> {
                                assertThat(errors.getError()).isEqualTo(Errors.UNKNOWN_SERVER_ERROR);
                                assertThat(errors).isInstanceOf(TopicNameMappingException.class);
                            });
                    assertThat(topicNamesMapping.topicNames()).containsExactly(entry(UUID, TOPIC_NAME));
                });
    }

    @Test
    void retrieveTopicNamesMappingHandlesSendFutureFailing() {
        // given
        RuntimeException exception = new RuntimeException("BOOM");
        givenSendRequestResponse(failedFuture(exception));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNamesMapping(Set.of(UUID));
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesMapping -> {
                    assertThat(topicNamesMapping.anyFailures()).isTrue();
                    assertThat(topicNamesMapping.failures()).containsOnlyKeys(UUID)
                            .hasEntrySatisfying(UUID, errors -> {
                                assertThat(errors.getError()).isEqualTo(Errors.UNKNOWN_SERVER_ERROR);
                                assertThat(errors).isInstanceOf(TopicNameMappingException.class);
                            });
                });
    }

    @Test
    void retrieveTopicNamesMappingUnexpectedResponseType() {
        // given
        ApiMessage response = new ApiVersionsResponseData();
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNamesMapping(Set.of(UUID));
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesMapping -> {
                    assertThat(topicNamesMapping.anyFailures()).isTrue();
                    assertThat(topicNamesMapping.failures()).containsOnlyKeys(UUID)
                            .hasEntrySatisfying(UUID, errors -> {
                                assertThat(errors.getError()).isEqualTo(Errors.UNKNOWN_SERVER_ERROR);
                                assertThat(errors).isInstanceOf(TopicNameMappingException.class);
                            });
                });
    }

    @Test
    void retrieveTopicNamesMappingEmptyResponse() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNamesMapping(Set.of(UUID));
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesMapping -> {
                    assertThat(topicNamesMapping.anyFailures()).isTrue();
                    assertThat(topicNamesMapping.failures()).containsOnlyKeys(UUID)
                            .hasEntrySatisfying(UUID, errors -> {
                                assertThat(errors.getError()).isEqualTo(Errors.UNKNOWN_SERVER_ERROR);
                                assertThat(errors).isInstanceOf(TopicNameMappingException.class);
                            });
                });
    }

    @Test
    void retrieveTopicNamesMappingTopLevelResponseError() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        response.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNamesMapping(Set.of(UUID));
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesMapping -> {
                    assertThat(topicNamesMapping.anyFailures()).isTrue();
                    assertThat(topicNamesMapping.failures()).containsOnlyKeys(UUID)
                            .hasEntrySatisfying(UUID, errors -> {
                                assertThat(errors.getError()).isEqualTo(Errors.UNKNOWN_SERVER_ERROR);
                                assertThat(errors).isInstanceOf(TopLevelMetadataErrorException.class);
                            });
                });
    }

    @Test
    void retrieveTopicNamesMappingTopicLevelError() {
        // given
        MetadataResponseData response = new MetadataResponseData();
        MetadataResponseData.MetadataResponseTopic responseTopic = new MetadataResponseData.MetadataResponseTopic().setTopicId(UUID)
                .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code());
        response.topics().add(responseTopic);
        givenSendRequestResponse(completedFuture(response));
        // when
        CompletionStage<TopicNameMapping> topicNames = getTopicNamesMapping(Set.of(UUID));
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesMapping -> {
                    assertThat(topicNamesMapping.anyFailures()).isTrue();
                    assertThat(topicNamesMapping.failures()).containsOnlyKeys(UUID)
                            .hasEntrySatisfying(UUID, e -> {
                                assertThat(e.getError()).isEqualTo(Errors.UNKNOWN_TOPIC_ID);
                                assertThat(e).isInstanceOf(TopicLevelMetadataErrorException.class);
                            });
                });
    }

    @Test
    void retrieveTopicNamesMappingMixtureOfSuccessAndTopicLevelFailure() {
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
        CompletionStage<TopicNameMapping> topicNames = getTopicNamesMapping(Set.of(UUID, UUID_2));
        // then
        assertThat(topicNames.toCompletableFuture()).succeedsWithin(Duration.ZERO)
                .satisfies(topicNamesMapping -> {
                    assertThat(topicNamesMapping.anyFailures()).isTrue();
                    assertThat(topicNamesMapping.topicNames()).containsExactly(entry(UUID_2, TOPIC_NAME_2));
                    assertThat(topicNamesMapping.failures()).containsOnlyKeys(UUID)
                            .hasEntrySatisfying(UUID, e -> {
                                assertThat(e.getError()).isEqualTo(Errors.UNKNOWN_TOPIC_ID);
                                assertThat(e).isInstanceOf(TopicLevelMetadataErrorException.class);
                            });
                });
    }

    private static MetadataResponseData.MetadataResponseTopic getResponseTopic(Uuid uuid, String topicName) {
        return new MetadataResponseData.MetadataResponseTopic()
                .setTopicId(uuid)
                .setName(topicName);
    }

    private void givenSendRequestResponse(CompletableFuture<ApiMessage> response) {
        // sendRequest returns a minimal stage that cannot be completed, or converted to future
        when(filterContext.sendRequest(any(), any())).thenReturn(response.minimalCompletionStage());
    }

    private CompletionStage<TopicNameMapping> getTopicNamesMapping(Set<Uuid> topicIds) {
        return retriever.topicNames(topicIds);
    }

}
