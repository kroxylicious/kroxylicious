/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarking.jmh.filters;

import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.ProduceResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import org.openjdk.jmh.infra.Blackhole;

import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import static io.kroxylicious.benchmarking.jmh.InvokerDispatchBenchmark.CONSUME_TOKENS;

public class FourInterfaceFilter1 implements ProduceResponseFilter, ProduceRequestFilter, ApiVersionsRequestFilter, ApiVersionsResponseFilter {
    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
        Blackhole.consumeCPU(CONSUME_TOKENS);
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onProduceResponse(short apiVersion, ResponseHeaderData header, ProduceResponseData response,
                                                                   FilterContext context) {
        Blackhole.consumeCPU(CONSUME_TOKENS);
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<RequestFilterResult> onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request,
                                                                     FilterContext context) {
        Blackhole.consumeCPU(CONSUME_TOKENS);
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData response,
                                                                       FilterContext context) {
        Blackhole.consumeCPU(CONSUME_TOKENS);
        return context.forwardResponse(header, response);
    }
}
