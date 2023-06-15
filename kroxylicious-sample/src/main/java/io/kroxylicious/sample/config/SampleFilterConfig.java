/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.BaseConfig;

/**
 * The Jackson configuration object for both the sample filters.<br />
 * Both filters perform the same transformation process (though on different types of messages and at
 * different points), only replacing one configured String value with another single configured String value,
 * meaning they can share a single configuration class.<br />
 * <br />
 * This configuration class accepts two String arguments: the value to be replaced, and the value it will be
 * replaced with.
 */
public class SampleFilterConfig extends BaseConfig {

    private final String from;
    private final String to;

    /**
     * @param from the value to be replaced
     * @param to the replacement value
     */
    public SampleFilterConfig(@JsonProperty(required = true) String from, @JsonProperty(required = true) String to) {
        this.from = from;
        this.to = to;
    }

    /**
     * @return the configured value to be replaced
     */
    public String getFrom() {
        return from;
    }

    /**
     * @return the configured replacement value
     */
    public String getTo() {
        return to;
    }
}
