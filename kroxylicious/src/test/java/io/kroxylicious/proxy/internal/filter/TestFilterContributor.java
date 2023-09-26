/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterContributor;

import edu.umd.cs.findbugs.annotations.NonNull;

public class TestFilterContributor {

    public static class ContributorA implements FilterContributor<ExampleConfig> {

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
            return new TestFilter(context, context.getConfig(), this.getClass());
        }
    }

    public static class ContributorB implements FilterContributor<ExampleConfig> {

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
            return new TestFilter(context, context.getConfig(), this.getClass());
        }
    }

    public static class RequiredConfigContributor implements FilterContributor<ExampleConfig> {

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
            return new TestFilter(context, context.getConfig(), this.getClass());
        }
    }

    public static class OptionalConfigContributor implements FilterContributor<ExampleConfig> {

        @NonNull
        @Override
        public Class<ExampleConfig> getConfigType() {
            return ExampleConfig.class;
        }

        @NonNull
        @Override
        public boolean requiresConfiguration() {
            return false;
        }

        @Override
        public Filter createInstance(FilterConstructContext<ExampleConfig> context) {
            return new TestFilter(context, context.getConfig(), this.getClass());
        }
    }

}
