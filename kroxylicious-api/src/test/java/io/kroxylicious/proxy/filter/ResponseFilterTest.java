/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class ResponseFilterTest {

    @Mock
    private FilterContext responseContext;
    @Mock
    private ResponseFilterResult responseFilterResult;

    @Test
    void newDefaultMethodImplChainsToDeprecatedMethod() {
        var responseHeader = new ResponseHeaderData();
        var responseData = new ProduceResponseData();

        var filterImplementingOldApi = new ResponseFilter() {
            @SuppressWarnings("removal")
            @Override
            public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {
                assertThat(apiKey).isEqualTo(ApiKeys.PRODUCE);
                assertThat(header).isSameAs(responseHeader);
                assertThat(response).isSameAs(responseData);
                assertThat(context).isSameAs(responseContext);

                return CompletableFuture.completedStage(responseFilterResult);

            }
        };

        var result = filterImplementingOldApi.onResponse(ApiKeys.PRODUCE, ApiKeys.PRODUCE.latestVersion(), responseHeader, responseData, responseContext);
        assertThat(result)
                .succeedsWithin(1, TimeUnit.SECONDS)
                .isSameAs(responseFilterResult);
    }
}
