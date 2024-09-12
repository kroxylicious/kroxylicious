/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class FilterInvokersTest {

    @ParameterizedTest
    @MethodSource("invalidFilters")
    void testInvalidFilters(Filter invalid) {
        Assertions.assertThrows(IllegalArgumentException.class, () -> FilterInvokers.from(invalid));
    }

    @ParameterizedTest
    @MethodSource("validFilters")
    void testValidFilters(Filter invalid) {
        assertThat(FilterInvokers.from(invalid)).isNotNull();
    }

    public static Stream<Filter> invalidFilters() {
        Filter noFilterSubinterfacesImplemented = new Filter() {

        };
        return Stream.of(
                noFilterSubinterfacesImplemented,
                new SpecificAndRequestFilter(),
                new SpecificAndResponseFilter()
        );
    }

    public static Stream<Filter> validFilters() {
        ApiVersionsRequestFilter singleSpecificMessageFilter = (apiVersion, header, request, context) -> null;
        RequestFilter requestFilter = (apiKey, header, body, filterContext) -> null;
        ResponseFilter responseFilter = (apiKey, header, body, filterContext) -> null;
        return Stream.of(
                singleSpecificMessageFilter,
                new MultipleSpecificFilter(),
                requestFilter,
                responseFilter,
                new RequestResponseFilter()
        );
    }

    static class RequestResponseFilter implements RequestFilter, ResponseFilter {

        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
            return null;
        }

        @Override
        public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {
            return null;
        }
    }

    static class MultipleSpecificFilter implements ApiVersionsRequestFilter, ApiVersionsResponseFilter {

        @Override
        public CompletionStage<RequestFilterResult> onApiVersionsRequest(
                short apiVersion,
                RequestHeaderData header,
                ApiVersionsRequestData request,
                FilterContext context
        ) {

            return null;
        }

        @Override
        public CompletionStage<ResponseFilterResult> onApiVersionsResponse(
                short apiVersion,
                ResponseHeaderData header,
                ApiVersionsResponseData response,
                FilterContext context
        ) {

            return null;
        }
    }

    static class SpecificAndResponseFilter implements ApiVersionsRequestFilter, ResponseFilter {

        @Override
        public CompletionStage<RequestFilterResult> onApiVersionsRequest(
                short apiVersion,
                RequestHeaderData header,
                ApiVersionsRequestData request,
                FilterContext context
        ) {

            return null;
        }

        @Override
        public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {

            return null;
        }
    }

    static class SpecificAndRequestFilter implements ApiVersionsRequestFilter, RequestFilter {

        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {

            return null;
        }

        @Override
        public CompletionStage<RequestFilterResult> onApiVersionsRequest(
                short apiVersion,
                RequestHeaderData header,
                ApiVersionsRequestData request,
                FilterContext context
        ) {

            return null;
        }
    }

}
