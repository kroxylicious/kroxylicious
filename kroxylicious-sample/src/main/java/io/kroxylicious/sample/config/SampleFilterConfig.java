/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Jackson configuration object for both the sample filters.<br />
 * Both filters perform the same transformation process (though on different types of messages and at
 * different points), only replacing one configured String value with another single configured String value,
 * meaning they can share a single configuration class.<br />
 * <br />
 * This configuration class accepts two String arguments: the value to be replaced, and the value it will be
 * replaced with.
 */
public class SampleFilterConfig {

    private final String findValue;
    private final String replacementValue;

    /**
     * @param findValue the value to be replaced
     * @param replacementValue the replacement value
     */
    public SampleFilterConfig(@JsonProperty(required = true)
    String findValue, @JsonProperty(required = false)
    String replacementValue) {
        this.findValue = findValue;
        this.replacementValue = replacementValue == null ? "" : replacementValue;
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
    public String getReplacementValue() {
        return replacementValue;
    }
}
