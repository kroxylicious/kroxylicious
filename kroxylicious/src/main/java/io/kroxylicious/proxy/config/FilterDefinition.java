/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

import io.kroxylicious.proxy.internal.filter.FilterConfig;

public class FilterDefinition {

    private final String type;
    private final FilterConfig config;

    @JsonCreator
    public FilterDefinition(String type,
                            @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type") @JsonTypeIdResolver(FilterConfigTypeIdResolver.class) FilterConfig config) {
        this.type = type;
        this.config = config;
    }

    public String type() {
        return type;
    }

    public FilterConfig config() {
        return config;
    }
}
