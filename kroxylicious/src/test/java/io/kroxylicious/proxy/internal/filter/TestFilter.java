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

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class TestFilter implements RequestFilter {
    public static final String SHORT_NAME_A = "TEST1";
    public static final String SHORT_NAME_B = "TEST2";
    private final String shortName;
    private final FilterConstructContext context;
    private final ExampleConfig exampleConfig;

    public TestFilter(String shortName, FilterConstructContext context, ExampleConfig exampleConfig) {
        this.shortName = shortName;
        this.context = context;
        this.exampleConfig = exampleConfig;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        throw new IllegalStateException("not implemented!");
    }

    public String getShortName() {
        return shortName;
    }

    public FilterConstructContext getContext() {
        return context;
    }

    public ExampleConfig getExampleConfig() {
        return exampleConfig;
    }

    public static class ContributorA implements FilterContributor {

        @Override
        public String getTypeName() {
            return SHORT_NAME_A;
        }

        @Override
        public Class<? extends BaseConfig> getConfigClass() {
            return ExampleConfig.class;
        }

        @Override
        public Filter getInstance(BaseConfig config, FilterConstructContext context) {
            return new TestFilter(SHORT_NAME_A, context, (ExampleConfig) config);
        }
    }

    public static class ContributorB implements FilterContributor {

        @Override
        public String getTypeName() {
            return SHORT_NAME_B;
        }

        @Override
        public Class<? extends BaseConfig> getConfigClass() {
            return ExampleConfig.class;
        }

        @Override
        public Filter getInstance(BaseConfig config, FilterConstructContext context) {
            return new TestFilter(SHORT_NAME_B, context, (ExampleConfig) config);
        }
    }
}
