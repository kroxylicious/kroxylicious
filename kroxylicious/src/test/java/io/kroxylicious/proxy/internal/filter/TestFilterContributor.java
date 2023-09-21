/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.service.ConfigurationDefinition;

public class TestFilterContributor {
    public static final String TYPE_NAME_A = "TEST1";
    public static final String TYPE_NAME_B = "TEST2";
    public static final String OPTIONAL_CONFIG_FILTER = "TEST3";
    public static final String REQUIRED_CONFIG_FILTER = "TEST4";

    public static class ContributorA implements FilterContributor {
        @Override
        public String getTypeName() {
            return TYPE_NAME_A;
        }

        @Override
        public ConfigurationDefinition getConfigDefinition() {
            return new ConfigurationDefinition(ExampleConfig.class, true);
        }

        @Override
        public Filter getInstance(FilterConstructContext context) {
            return new TestFilter(getTypeName(), context, (ExampleConfig) context.getConfig());
        }
    }

    public static class ContributorB implements FilterContributor {
        @Override
        public String getTypeName() {
            return TYPE_NAME_B;
        }

        @Override
        public ConfigurationDefinition getConfigDefinition() {
            return new ConfigurationDefinition(ExampleConfig.class, true);
        }

        @Override
        public Filter getInstance(FilterConstructContext context) {
            return new TestFilter(getTypeName(), context, (ExampleConfig) context.getConfig());
        }
    }

    public static class RequiredConfigContributor implements FilterContributor {
        @Override
        public String getTypeName() {
            return REQUIRED_CONFIG_FILTER;
        }

        @Override
        public ConfigurationDefinition getConfigDefinition() {
            return new ConfigurationDefinition(ExampleConfig.class, true);
        }

        @Override
        public Filter getInstance(FilterConstructContext context) {
            return new TestFilter(getTypeName(), context, (ExampleConfig) context.getConfig());
        }
    }

    public static class OptionalConfigContributor implements FilterContributor {
        @Override
        public String getTypeName() {
            return OPTIONAL_CONFIG_FILTER;
        }

        @Override
        public ConfigurationDefinition getConfigDefinition() {
            return new ConfigurationDefinition(ExampleConfig.class, false);
        }

        @Override
        public Filter getInstance(FilterConstructContext context) {
            return new TestFilter(getTypeName(), context, (ExampleConfig) context.getConfig());
        }
    }

}