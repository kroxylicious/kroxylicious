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
import org.jetbrains.annotations.NotNull;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.service.Contributor;

import edu.umd.cs.findbugs.annotations.NonNull;

public class RequiresConfigFilter implements RequestFilter {
    private final FilterConstructContext context;
    private final ExampleConfig exampleConfig;
    private final Class<? extends Contributor<?, ?, ?>> contributorClass;

    public RequiresConfigFilter(FilterConstructContext context, ExampleConfig exampleConfig, Class<? extends Contributor<?, ?, ?>> contributorClass) {
        this.context = context;
        this.exampleConfig = exampleConfig;
        this.contributorClass = contributorClass;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        throw new IllegalStateException("not implemented!");
    }

    public FilterConstructContext getContext() {
        return context;
    }

    public ExampleConfig getExampleConfig() {
        return exampleConfig;
    }

    public Class<? extends Contributor<?, ?, ?>> getContributorClass() {
        return contributorClass;
    }

    public static class RequiresConfigContributor implements FilterContributor<ExampleConfig> {

        @NotNull
        @Override
        public Class<? extends Filter> getServiceType() {
            return RequiresConfigFilter.class;
        }

        @NonNull
        @Override
        public Class<ExampleConfig> getConfigType() {
            return ExampleConfig.class;
        }

        @NonNull
        @Override
        public boolean requiresConfiguration() {
            return true;
        }

        @Override
        public Filter createInstance(FilterConstructContext<ExampleConfig> context) {
            return new RequiresConfigFilter(context, context.getConfig(), this.getClass());
        }
    }
}
