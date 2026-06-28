/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import com.fasterxml.jackson.annotation.JsonCreator;

public class ConstructorInjectionConfig {
    private final String str;

    @JsonCreator
    public ConstructorInjectionConfig(String str) {
        this.str = str;
    }

    public String str() {
        return str;
    }
}
