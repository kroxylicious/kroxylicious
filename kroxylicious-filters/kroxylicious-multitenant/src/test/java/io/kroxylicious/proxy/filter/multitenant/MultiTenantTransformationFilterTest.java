/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.multitenant.config.MultiTenantConfig;
import io.kroxylicious.test.condition.kafka.FetchResponseDataCondition;

import static io.kroxylicious.test.condition.kafka.ApiMessageCondition.forApiKey;
import static io.kroxylicious.test.condition.kafka.ProduceRequestDataCondition.produceRequestMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MultiTenantTransformationFilterTest {

    @Mock
    private FilterContext filterContext;

    @BeforeEach
    void setUp() {
    }

    @Test
    void shouldReWriteTopic() {
        var multiTenantTransformationFilter = new MultiTenantTransformationFilter(new MultiTenantConfig(null));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn("vc1");
        final ProduceRequestData request = new ProduceRequestData();
        final ProduceRequestData.TopicProduceData topicData = new ProduceRequestData.TopicProduceData();
        topicData.setName("testTopic");
        request.topicData().add(topicData);
        // When
        multiTenantTransformationFilter.onProduceRequest(
                ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), request, filterContext);

        // Then
        verify(filterContext).forwardRequest(any(RequestHeaderData.class), assertArg(
                apiMessage -> assertThat(apiMessage)
                        .is(forApiKey(ApiKeys.PRODUCE))
                        .is(produceRequestMatching(produceRequestData -> produceRequestData.topicData()
                                .stream()
                                .allMatch(topicProduceData -> topicProduceData.name().equals("vc1-testTopic"))))));

    }

    @Test
    void produceWithCustomSeparator() {
        var multiTenantTransformationFilter = new MultiTenantTransformationFilter(new MultiTenantConfig("."));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn("vc1");
        final ProduceRequestData request = new ProduceRequestData();
        final ProduceRequestData.TopicProduceData topicData = new ProduceRequestData.TopicProduceData();
        topicData.setName("testTopic");
        request.topicData().add(topicData);
        // When
        multiTenantTransformationFilter.onProduceRequest(
                ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), request, filterContext);

        // Then
        verify(filterContext).forwardRequest(any(RequestHeaderData.class), assertArg(
                apiMessage -> assertThat(apiMessage)
                        .is(forApiKey(ApiKeys.PRODUCE))
                        .is(produceRequestMatching(produceRequestData -> produceRequestData.topicData()
                                .stream()
                                .allMatch(topicProduceData -> topicProduceData.name().equals("vc1.testTopic"))))));

    }

    @Test
    void fetchWithCustomSeparator() {
        var multiTenantTransformationFilter = new MultiTenantTransformationFilter(new MultiTenantConfig("."));
        // Given
        when(filterContext.getVirtualClusterName()).thenReturn("vc1");
        var response = new FetchResponseData();
        var topicResponse = new FetchResponseData.FetchableTopicResponse();
        topicResponse.setTopic("vc1.testTopic");
        response.responses().add(topicResponse);
        // When
        multiTenantTransformationFilter.onFetchResponse(
                FetchResponseData.HIGHEST_SUPPORTED_VERSION, new ResponseHeaderData(), response, filterContext);

        // Then
        verify(filterContext).forwardResponse(any(ResponseHeaderData.class), assertArg(
                apiMessage -> assertThat(apiMessage)
                        .is(forApiKey(ApiKeys.FETCH))
                        .is(FetchResponseDataCondition.fetchResponseMatching(fetchResponseData -> fetchResponseData.responses()
                                .stream()
                                .allMatch(topicFetchData -> topicFetchData.topic().equals("testTopic"))))));

    }
}
