/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import com.fasterxml.jackson.annotation.JsonCreator;

public class FactoryMethodConfig {

    private final String str;

    private FactoryMethodConfig(String str) {
        this.str = str;
    }

    @JsonCreator
    public static FactoryMethodConfig factoryMethod(String str) {
        return new FactoryMethodConfig(str);
    }

    public String str() {
        return str;
    }
}
