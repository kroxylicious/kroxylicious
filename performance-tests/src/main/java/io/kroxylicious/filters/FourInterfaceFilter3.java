/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.openjdk.jmh.infra.Blackhole;

import io.kroxylicious.proxy.filter.FetchRequestFilter;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterResult;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import static io.kroxylicious.benchmarks.InvokerDispatchBenchmark.CONSUME_TOKENS;

public class FourInterfaceFilter3 implements ProduceResponseFilter, ProduceRequestFilter, FetchRequestFilter, FetchResponseFilter {

    @Override
    public CompletionStage<FilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, KrpcFilterContext context) {
        Blackhole.consumeCPU(CONSUME_TOKENS);
        return null;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onProduceResponse(short apiVersion, ResponseHeaderData header, ProduceResponseData response,
                                                                   KrpcFilterContext context) {
        Blackhole.consumeCPU(CONSUME_TOKENS);
        return null;
    }

    @Override
    public CompletionStage<FilterResult> onFetchRequest(short apiVersion, RequestHeaderData header, FetchRequestData request, KrpcFilterContext context) {
        Blackhole.consumeCPU(CONSUME_TOKENS);
        return null;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFetchResponse(short apiVersion, ResponseHeaderData header, FetchResponseData response, KrpcFilterContext context) {
        Blackhole.consumeCPU(CONSUME_TOKENS);
        return null;
    }
}
