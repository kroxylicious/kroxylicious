/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import java.util.Objects;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.multitenant.config.MultiTenantConfig;
import io.kroxylicious.test.condition.kafka.FetchResponseDataCondition;

import static io.kroxylicious.test.condition.kafka.ApiMessageCondition.forApiKey;
import static io.kroxylicious.test.condition.kafka.ProduceRequestDataCondition.produceRequestMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MultiTenantTransformationFilterTest {

    private static final String VIRTUAL_CLUSTER_NAME = "vc1";
    private static final String TEST_TOPIC = "testTopic";
    @Mock
    private FilterContext filterContext;

    @Test
    void shouldReWriteTopic() {
        var multiTenantTransformationFilter = new MultiTenantTransformationFilter(new MultiTenantConfig(null));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn(VIRTUAL_CLUSTER_NAME);
        var request = createProduceRequest(TEST_TOPIC);
        // When
        multiTenantTransformationFilter.onProduceRequest(
                ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(),
                request,
                filterContext
        );

        // Then
        verify(filterContext).forwardRequest(
                any(RequestHeaderData.class),
                assertArg(
                        apiMessage -> assertThat(apiMessage)
                                                            .is(forApiKey(ApiKeys.PRODUCE))
                                                            .is(
                                                                    produceRequestMatching(
                                                                            produceRequestData -> produceRequestData.topicData()
                                                                                                                    .stream()
                                                                                                                    .allMatch(
                                                                                                                            topicProduceData -> topicProduceData.name()
                                                                                                                                                                .equals(
                                                                                                                                                                        "vc1-testTopic"
                                                                                                                                                                )
                                                                                                                    )
                                                                    )
                                                            )
                )
        );

    }

    @ParameterizedTest
    @ValueSource(strings = { ".", "_", "", "-.-" })
    @NullSource
    void produceWithCustomSeparator(String separator) {
        var expectedServerTopicName = "%s%s%s".formatted(VIRTUAL_CLUSTER_NAME, Objects.requireNonNullElse(separator, MultiTenantConfig.DEFAULT_SEPARATOR), TEST_TOPIC);
        var multiTenantTransformationFilter = new MultiTenantTransformationFilter(new MultiTenantConfig(separator));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn(VIRTUAL_CLUSTER_NAME);
        var request = createProduceRequest(TEST_TOPIC);
        // When
        multiTenantTransformationFilter.onProduceRequest(
                ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(),
                request,
                filterContext
        );

        // Then
        verify(filterContext).forwardRequest(
                any(RequestHeaderData.class),
                assertArg(
                        apiMessage -> assertThat(apiMessage)
                                                            .is(forApiKey(ApiKeys.PRODUCE))
                                                            .is(
                                                                    produceRequestMatching(
                                                                            produceRequestData -> produceRequestData.topicData()
                                                                                                                    .stream()
                                                                                                                    .allMatch(
                                                                                                                            topicProduceData -> topicProduceData.name()
                                                                                                                                                                .equals(
                                                                                                                                                                        expectedServerTopicName
                                                                                                                                                                )
                                                                                                                    )
                                                                    )
                                                            )
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = { ".", "_", "", "-.-" })
    @NullSource
    void fetchWithCustomSeparator(String separator) {
        var serverTopic = "%s%s%s".formatted(VIRTUAL_CLUSTER_NAME, Objects.requireNonNullElse(separator, MultiTenantConfig.DEFAULT_SEPARATOR), TEST_TOPIC);
        var multiTenantTransformationFilter = new MultiTenantTransformationFilter(new MultiTenantConfig(separator));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn(VIRTUAL_CLUSTER_NAME);
        var response = createFetchResponseData(serverTopic);
        // When
        multiTenantTransformationFilter.onFetchResponse(
                FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                new ResponseHeaderData(),
                response,
                filterContext
        );

        // Then
        verify(filterContext).forwardResponse(
                any(ResponseHeaderData.class),
                assertArg(
                        apiMessage -> assertThat(apiMessage)
                                                            .is(forApiKey(ApiKeys.FETCH))
                                                            .is(
                                                                    FetchResponseDataCondition.fetchResponseMatching(
                                                                            fetchResponseData -> fetchResponseData.responses()
                                                                                                                  .stream()
                                                                                                                  .allMatch(
                                                                                                                          topicFetchData -> topicFetchData.topic()
                                                                                                                                                          .equals(
                                                                                                                                                                  TEST_TOPIC
                                                                                                                                                          )
                                                                                                                  )
                                                                    )
                                                            )
                )
        );
    }

    @Test
    void illegalKafkaResourceCharsInVirtualClusterNameDetected() {
        var multiTenantTransformationFilter = new MultiTenantTransformationFilter(new MultiTenantConfig(MultiTenantConfig.DEFAULT_SEPARATOR));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn("$badvcn$");
        var request = createProduceRequest(TEST_TOPIC);
        var header = new RequestHeaderData();
        // When
        assertThatThrownBy(() -> {
            multiTenantTransformationFilter.onProduceRequest(
                    ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                    header,
                    request,
                    filterContext
            );
        }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void illegalKafkaResourceCharsInSeparatorDetected() {
        var multiTenantTransformationFilter = new MultiTenantTransformationFilter(new MultiTenantConfig("$bad$"));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn(VIRTUAL_CLUSTER_NAME);
        var request = createProduceRequest(TEST_TOPIC);
        var header = new RequestHeaderData();
        // When
        assertThatThrownBy(() -> {
            multiTenantTransformationFilter.onProduceRequest(
                    ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                    header,
                    request,
                    filterContext
            );
        }).isInstanceOf(IllegalStateException.class);
    }

    private ProduceRequestData createProduceRequest(String topic) {
        final ProduceRequestData request = new ProduceRequestData();
        final ProduceRequestData.TopicProduceData topicData = new ProduceRequestData.TopicProduceData();
        topicData.setName(topic);
        request.topicData().add(topicData);
        return request;
    }

    private FetchResponseData createFetchResponseData(String topic) {
        var response = new FetchResponseData();
        var topicResponse = new FetchResponseData.FetchableTopicResponse();
        topicResponse.setTopic(topic);
        response.responses().add(topicResponse);
        return response;
    }

}
