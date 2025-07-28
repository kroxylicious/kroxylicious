/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import edu.umd.cs.findbugs.annotations.NonNull;

public class ProtocolCounterFilter implements
        RequestFilter,
        ResponseFilter {

    private static @NonNull byte[] toBytes(int count) {
        return new byte[]{
                (byte) ((count & 0xFF000000) >> 24),
                (byte) ((count & 0x00FF0000) >> 16),
                (byte) ((count & 0x0000FF00) >> 8),
                (byte) count,
        };
    }

    public static int fromBytes(@NonNull byte[] bytes) {
        return bytes[0] << 24 | bytes[1] << 16 | bytes[2] << 8 | bytes[3];
    }

    public static String requestCountHeaderKey(ApiKeys apiKey) {
        return AbstractProduceHeaderInjectionFilter.headerName("#request." + apiKey);
    }

    public static String responseCountHeaderKey(ApiKeys apiKey) {
        return AbstractProduceHeaderInjectionFilter.headerName("#response." + apiKey);
    }

    private final EnumMap<ApiKeys, Integer> requestApisToCount;
    private final EnumMap<ApiKeys, Integer> responseApisToCount;
    private final AbstractProduceHeaderInjectionFilter headerInjector = new AbstractProduceHeaderInjectionFilter() {

        @NonNull
        @Override
        protected List<RecordHeader> headersToAdd(FilterContext context) {
            var result = new ArrayList<RecordHeader>();
            requestApisToCount.forEach((apiKey, count) -> {
                result.add(new RecordHeader(requestCountHeaderKey(apiKey), toBytes(count)));
            });
            responseApisToCount.forEach((apiKey, count) -> {
                result.add(new RecordHeader(responseCountHeaderKey(apiKey), toBytes(count)));
            });
            return result;
        }
    };

    public ProtocolCounterFilter(EnumMap<ApiKeys, Integer> requestApisToCount, EnumMap<ApiKeys, Integer> responseApisToCount) {
        this.requestApisToCount = requestApisToCount;
        this.responseApisToCount = responseApisToCount;
    }


    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        Integer count = requestApisToCount.get(apiKey);
        if (count != null) {
            count = count + 1;
            requestApisToCount.put(apiKey, count);
        }

        if (apiKey == ApiKeys.PRODUCE) {
            headerInjector.onProduceRequest(header.requestApiVersion(), header, (ProduceRequestData) request, context);
        }

        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {
        Integer count = responseApisToCount.get(apiKey);
        if (count != null) {
            count = count + 1;
            responseApisToCount.put(apiKey, count);
        }
        return context.forwardResponse(header, response);
    }
}
