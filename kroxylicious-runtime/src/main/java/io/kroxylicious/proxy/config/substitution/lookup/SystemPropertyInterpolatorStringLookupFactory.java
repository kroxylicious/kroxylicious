/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.substitution.lookup;

public class SystemPropertyInterpolatorStringLookupFactory implements InterpolatorLookupFactory {
    @Override
    public String getType() {
        return "sys";
    }

    @Override
    public StringLookup create() {
        return FunctionStringLookup.on(System::getProperty);
    }
}
