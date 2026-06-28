/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

public interface ExamplePluginFactory<C> {

    ExamplePlugin createExamplePlugin(C configuration);

    @FunctionalInterface
    interface ExamplePlugin {
        void foo();
    }
}
