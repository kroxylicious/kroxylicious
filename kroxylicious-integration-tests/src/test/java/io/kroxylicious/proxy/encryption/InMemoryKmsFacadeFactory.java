/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

public class InMemoryKmsFacadeFactory implements TestKmsFacadeFactory {
    @Override
    public TestKmsFacade build() {
        return new InMemoryKmsFacade();
    }
}
