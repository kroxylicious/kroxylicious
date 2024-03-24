/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.substitution.lookup;

public class EnvironmentInterpolatorStringLookupFactory implements InterpolatorLookupFactory {
    @Override
    public String getType() {
        return "env";
    }

    @Override
    public StringLookup create() {
        return FunctionStringLookup.on(System::getenv);
    }
}
