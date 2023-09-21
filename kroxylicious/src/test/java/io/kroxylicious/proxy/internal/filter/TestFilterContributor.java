/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.ConfigurableFilterContributor;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;

public class TestFilterContributor {
    public static final String TYPE_NAME_A = "TEST1";
    public static final String TYPE_NAME_B = "TEST2";
    public static final String OPTIONAL_CONFIG_FILTER = "TEST3";
    public static final String REQUIRED_CONFIG_FILTER = "TEST4";

    public static class ContributorA extends ConfigurableFilterContributor<ExampleConfig> {

        public ContributorA() {
            super(TYPE_NAME_A, ExampleConfig.class, true);
        }

        @Override
        protected Filter getInstance(FilterConstructContext context, ExampleConfig config) {
            return new TestFilter(getTypeName(), context, config);
        }
    }

    public static class ContributorB extends ConfigurableFilterContributor<ExampleConfig> {

        public ContributorB() {
            super(TYPE_NAME_B, ExampleConfig.class, true);
        }

        @Override
        protected Filter getInstance(FilterConstructContext context, ExampleConfig config) {
            return new TestFilter(getTypeName(), context, config);
        }
    }

    public static class RequiredConfigContributor extends ConfigurableFilterContributor<ExampleConfig> {

        public RequiredConfigContributor() {
            super(REQUIRED_CONFIG_FILTER, ExampleConfig.class, true);
        }

        @Override
        protected Filter getInstance(FilterConstructContext context, ExampleConfig config) {
            return new TestFilter(getTypeName(), context, config);
        }
    }

    public static class OptionalConfigContributor extends ConfigurableFilterContributor<ExampleConfig> {

        public OptionalConfigContributor() {
            super(OPTIONAL_CONFIG_FILTER, ExampleConfig.class, false);
        }

        @Override
        protected Filter getInstance(FilterConstructContext context, ExampleConfig config) {
            return new TestFilter(getTypeName(), context, config);
        }
    }
}