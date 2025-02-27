/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.secret;

public class SecuritySensitive {

    private final String locator;
    // scheme:path:key

    public SecuritySensitive(String locator) {
        this.locator = locator;
    }

    public String scheme() {
        return locator;
    }

    public String path() {
        return locator;
    }

    public String key() {
        return locator;
    }

    public String locator() {
        return locator;
    }

    public String toString() {
        return "****** (security sensitive)";
    }

}
