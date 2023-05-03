/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;

public class OneInterfaceFilter implements ProduceResponseFilter {

    @Override
    public void onProduceResponse(ResponseHeaderData header, ProduceResponseData response, KrpcFilterContext context) {
    }
}
