/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import io.kroxylicious.proxy.filter.FilterFactory;

public class FilterPlugin extends Plugin<FilterFactory<?, ?>> {
    private final String type;
    private final JsonNode config;

    @JsonCreator
    public FilterPlugin(@JsonProperty(required = true) String type,
                        JsonNode config) {
        super((Class) FilterFactory.class);
        this.type = type;
        this.config = config;
    }

    /**
     * The type of the filter
     * @return
     */
    public String type() {
        return type;
    }

    /**
     * The configuration for creating the filter
     * @return
     */
    public JsonNode config() {
        return config;
    }
}
