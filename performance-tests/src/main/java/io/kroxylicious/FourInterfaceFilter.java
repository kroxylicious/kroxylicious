/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.CreateTopicsRequestFilter;
import io.kroxylicious.proxy.filter.CreateTopicsResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;

public class FourInterfaceFilter implements ProduceResponseFilter, ProduceRequestFilter, CreateTopicsRequestFilter, CreateTopicsResponseFilter {

    @Override
    public void onProduceRequest(RequestHeaderData header, ProduceRequestData request, KrpcFilterContext context) {
    }

    @Override
    public void onProduceResponse(ResponseHeaderData header, ProduceResponseData response, KrpcFilterContext context) {
    }

    @Override
    public void onCreateTopicsRequest(RequestHeaderData header, CreateTopicsRequestData request, KrpcFilterContext context) {

    }

    @Override
    public void onCreateTopicsResponse(ResponseHeaderData header, CreateTopicsResponseData response, KrpcFilterContext context) {

    }
}
