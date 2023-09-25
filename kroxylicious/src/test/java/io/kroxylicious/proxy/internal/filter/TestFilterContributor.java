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
    public static final String TYPE_NAME_A = "TEST1";
    public static final String TYPE_NAME_B = "TEST2";
    public static final String OPTIONAL_CONFIG_FILTER = "TEST3";
    public static final String REQUIRED_CONFIG_FILTER = "TEST4";

    public static class ContributorA implements FilterContributor<ExampleConfig> {
        @Override
        public String getTypeName() {
            return TYPE_NAME_A;
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
            return new TestFilter(getTypeName(), context, context.getConfig());
        }
    }

    public static class ContributorB implements FilterContributor<ExampleConfig> {
        @Override
        public String getTypeName() {
            return TYPE_NAME_B;
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
            return new TestFilter(getTypeName(), context, context.getConfig());
        }
    }

    public static class RequiredConfigContributor implements FilterContributor<ExampleConfig> {
        @Override
        public String getTypeName() {
            return REQUIRED_CONFIG_FILTER;
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
            return new TestFilter(getTypeName(), context, context.getConfig());
        }
    }

    public static class OptionalConfigContributor implements FilterContributor<ExampleConfig> {
        @Override
        public String getTypeName() {
            return OPTIONAL_CONFIG_FILTER;
        }

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
            return new TestFilter(getTypeName(), context, context.getConfig());
        }
    }

}
