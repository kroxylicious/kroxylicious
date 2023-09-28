/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class TestFilter implements RequestFilter {
    private final FilterCreationContext context;
    private final ExampleConfig exampleConfig;
    private final Class<? extends FilterFactory> contributorClass;

    public TestFilter(FilterCreationContext context, ExampleConfig exampleConfig, Class<? extends FilterFactory> contributorClass) {
        this.context = context;
        this.exampleConfig = exampleConfig;
        this.contributorClass = contributorClass;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        throw new IllegalStateException("not implemented!");
    }

    public FilterCreationContext getContext() {
        return context;
    }

    public ExampleConfig getExampleConfig() {
        return exampleConfig;
    }

    public Class<? extends FilterFactory> getContributorClass() {
        return contributorClass;
    }

}
