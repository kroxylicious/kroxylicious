/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

public class OneInterfaceFilter implements ProduceResponseFilter {

    @Override
    public CompletionStage<ResponseFilterResult> onProduceResponse(
            short apiVersion,
            ResponseHeaderData header,
            ProduceResponseData response,
            FilterContext context
    ) {
        return null;
    }
}
