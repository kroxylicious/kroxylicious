/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

/**
 * A singleton anonymous {@link Principal}.
 */
public class Anonymous implements Principal {

    private static Anonymous INSTANCE = new Anonymous();

    public static Anonymous anonymous() {
        return INSTANCE;
    }

    private Anonymous() {
    }

    @Override
    public String name() {
        return "";
    }
}
