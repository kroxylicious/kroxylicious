/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.Objects;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.FilterContributorContext;
import io.kroxylicious.proxy.filter.KrpcFilter;

public class TestFilterContributor implements FilterContributor {

    public static final String SHORT_NAME = "test";

    @Override
    public Class<? extends BaseConfig> getConfigType(String shortName) {
        if (!Objects.equals(shortName, SHORT_NAME)) {
            return null;
        }
        else {
            return Config.class;
        }
    }

    @Override
    public KrpcFilter getInstance(String shortName, BaseConfig config, FilterContributorContext context) {
        if (!Objects.equals(shortName, SHORT_NAME)) {
            return null;
        }
        else {
            return new TestKrpcFilter(shortName, config, context);
        }
    }
}
