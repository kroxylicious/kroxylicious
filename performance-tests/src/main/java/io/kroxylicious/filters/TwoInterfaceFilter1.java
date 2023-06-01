/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;

public class TwoInterfaceFilter1 implements ProduceResponseFilter, ProduceRequestFilter {

    @Override
    public void onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, KrpcFilterContext context) {
    }

    @Override
    public void onProduceResponse(short apiVersion, ResponseHeaderData header, ProduceResponseData response, KrpcFilterContext context) {
    }
}
