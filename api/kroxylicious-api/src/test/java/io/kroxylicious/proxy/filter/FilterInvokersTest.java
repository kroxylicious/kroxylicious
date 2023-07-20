/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class FilterInvokersTest {

    @ParameterizedTest
    @MethodSource("invalidFilters")
    public void testInvalidFilters(KrpcFilter invalid) {
        Assertions.assertThrows(IllegalArgumentException.class, () -> FilterInvokers.from(invalid));
    }

    @ParameterizedTest
    @MethodSource("validFilters")
    public void testValidFilters(KrpcFilter invalid) {
        assertThat(FilterInvokers.from(invalid)).isNotNull();
    }

    @Test
    public void testCompositeFilter() {
        MultipleSpecificFilter filterA = new MultipleSpecificFilter();
        RequestResponseFilter filterB = new RequestResponseFilter();
        RequestFilter filterC = (apiKey, header, body, filterContext) -> {

        };
        ResponseFilter filterD = (apiKey, header, body, filterContext) -> {

        };
        List<FilterAndInvoker> from = FilterInvokers.from(
                (CompositeFilter) () -> List.of((CompositeFilter) () -> List.of(filterA, filterB), (CompositeFilter) () -> List.of(filterC, filterD)));
        List<KrpcFilter> filters = from.stream().map(FilterAndInvoker::filter).toList();
        assertThat(filters).containsExactly(filterA, filterB, filterC, filterD);
    }

    public static Stream<KrpcFilter> invalidFilters() {
        KrpcFilter noKrpcFilterSubinterfacesImplemented = new KrpcFilter() {

        };
        return Stream.of(noKrpcFilterSubinterfacesImplemented,
                new SpecificAndRequestFilter(),
                new SpecificAndResponseFilter(),
                new SpecificAndCompositeFilter(),
                new CompositeAndRequestFilter(),
                new CompositeAndResponseFilter(),
                new TooDeeplyNestedCompositeFilter(),
                new SelfReferencingCompositeFilter());
    }

    public static Stream<KrpcFilter> validFilters() {
        ApiVersionsRequestFilter singleSpecificMessageFilter = (apiVersion, header, request, context) -> {

        };
        RequestFilter requestFilter = (apiKey, header, body, filterContext) -> {

        };
        ResponseFilter responseFilter = (apiKey, header, body, filterContext) -> {

        };
        return Stream.of(singleSpecificMessageFilter,
                new MultipleSpecificFilter(),
                requestFilter,
                responseFilter,
                new RequestResponseFilter(),
                new NoRecursionCompositeFilter(),
                new SingleRecursionCompositeFilter());
    }

    static class RequestResponseFilter implements RequestFilter, ResponseFilter {

        @Override
        public void onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {

        }

        @Override
        public void onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {

        }
    }

    static class NoRecursionCompositeFilter implements CompositeFilter {

        @Override
        public List<KrpcFilter> getFilters() {
            return List.of(new MultipleSpecificFilter());
        }
    }

    static class SingleRecursionCompositeFilter implements CompositeFilter {

        @Override
        public List<KrpcFilter> getFilters() {
            return List.of((CompositeFilter) () -> List.of(new MultipleSpecificFilter()), (CompositeFilter) () -> List.of(new MultipleSpecificFilter()));
        }
    }

    static class MultipleSpecificFilter implements ApiVersionsRequestFilter, ApiVersionsResponseFilter {

        @Override
        public void onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request, KrpcFilterContext context) {

        }

        @Override
        public void onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData response, KrpcFilterContext context) {

        }
    }

    static class CompositeAndRequestFilter implements RequestFilter, CompositeFilter {
        @Override
        public List<KrpcFilter> getFilters() {
            return null;
        }

        @Override
        public void onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {

        }
    }

    /**
     * We do not want to explode Composite Filters forever
     */
    static class SelfReferencingCompositeFilter implements CompositeFilter {
        @Override
        public List<KrpcFilter> getFilters() {
            return List.of(this);
        }
    }

    static class TooDeeplyNestedCompositeFilter implements CompositeFilter {
        @Override
        public List<KrpcFilter> getFilters() {
            return List.of((CompositeFilter) () -> List.of((CompositeFilter) () -> List.of((ApiVersionsRequestFilter) (apiVersion, header, request, context) -> {

            })));
        }
    }

    static class CompositeAndResponseFilter implements ResponseFilter, CompositeFilter {
        @Override
        public List<KrpcFilter> getFilters() {
            return null;
        }

        @Override
        public void onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {

        }
    }

    static class SpecificAndCompositeFilter implements ApiVersionsRequestFilter, CompositeFilter {

        @Override
        public void onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request, KrpcFilterContext context) {

        }

        @Override
        public List<KrpcFilter> getFilters() {
            return null;
        }
    }

    static class SpecificAndResponseFilter implements ApiVersionsRequestFilter, ResponseFilter {

        @Override
        public void onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request, KrpcFilterContext context) {

        }

        @Override
        public void onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {

        }
    }

    static class SpecificAndRequestFilter implements ApiVersionsRequestFilter, RequestFilter {

        @Override
        public void onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {

        }

        @Override
        public void onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request, KrpcFilterContext context) {

        }
    }

}