/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.openjdk.jmh.infra.Blackhole;

import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;

public class FourInterfaceFilter0 implements ProduceResponseFilter, ProduceRequestFilter, ApiVersionsRequestFilter, ApiVersionsResponseFilter {

    private static final int CONSUME_TOKENS = 15;

    @Override
    public void onProduceRequest(RequestHeaderData header, ProduceRequestData request, KrpcFilterContext context) {
        Blackhole.consumeCPU(CONSUME_TOKENS);
    }

    @Override
    public void onProduceResponse(ResponseHeaderData header, ProduceResponseData response, KrpcFilterContext context) {
        Blackhole.consumeCPU(CONSUME_TOKENS);
    }

    @Override
    public void onApiVersionsRequest(RequestHeaderData header, ApiVersionsRequestData request, KrpcFilterContext context) {
    }

    @Override
    public void onApiVersionsResponse(ResponseHeaderData header, ApiVersionsResponseData response, KrpcFilterContext context) {
    }
}
