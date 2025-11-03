/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.microbenchmarks.filters;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.CreateTopicsRequestFilter;
import io.kroxylicious.proxy.filter.CreateTopicsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

public class FourInterfaceFilter implements ProduceResponseFilter, ProduceRequestFilter, CreateTopicsRequestFilter, CreateTopicsResponseFilter {

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
        return null;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onProduceResponse(short apiVersion, ResponseHeaderData header, ProduceResponseData response,
                                                                   FilterContext context) {
        return null;
    }

    @Override
    public CompletionStage<RequestFilterResult> onCreateTopicsRequest(short apiVersion, RequestHeaderData header, CreateTopicsRequestData request,
                                                                      FilterContext context) {

        return null;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onCreateTopicsResponse(short apiVersion, ResponseHeaderData header, CreateTopicsResponseData response,
                                                                        FilterContext context) {

        return null;
    }
}
