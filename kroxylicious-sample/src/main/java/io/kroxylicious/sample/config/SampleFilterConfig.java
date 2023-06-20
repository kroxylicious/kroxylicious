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

    private final String findValue;
    private final String replaceValue;

    /**
     * @param findValue the value to be replaced
     * @param replaceValue the replacement value
     */
    public SampleFilterConfig(@JsonProperty(required = true) String findValue, @JsonProperty(required = true) String replaceValue) {
        this.findValue = findValue;
        this.replaceValue = replaceValue;
    }

    /**
     * Returns the configured value to be replaced
     */
    public String getFindValue() {
        return findValue;
    }

    /**
     * Returns the configured replacement value
     */
    public String getReplaceValue() {
        return replaceValue;
    }
}
