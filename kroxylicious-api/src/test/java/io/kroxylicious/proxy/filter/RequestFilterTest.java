/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class RequestFilterTest {

    @Mock
    private FilterContext requestContext;
    @Mock
    private RequestFilterResult requestFilterResult;

    @Test
    void newDefaultMethodImplChainsToDeprecatedMethod() {
        var requestHeader = new RequestHeaderData();
        var requestData = new ProduceRequestData();

        var filterImplementingOldApi = new RequestFilter() {
            @Override
            @SuppressWarnings("removal")
            public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
                assertThat(apiKey).isEqualTo(ApiKeys.PRODUCE);
                assertThat(header).isSameAs(requestHeader);
                assertThat(request).isSameAs(requestData);
                assertThat(context).isSameAs(requestContext);

                return CompletableFuture.completedStage(requestFilterResult);
            }
        };

        var result = filterImplementingOldApi.onRequest(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion(), requestHeader, requestData, requestContext);
        assertThat(result)
                .succeedsWithin(1, TimeUnit.SECONDS)
                .isSameAs(requestFilterResult);
    }
}