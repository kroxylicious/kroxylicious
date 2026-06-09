/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

class RouterFactoryTest {

    private final RouterFactory<Void, String> factory = new RouterFactory<>() {
        @Override
        public String initialize(RouterFactoryContext context, Void config) {
            return "init-data";
        }

        @Override
        public Router createRouter(RouterFactoryContext context, String initializationData) {
            return null;
        }
    };

    @Test
    void defaultCloseIsNoOp() {
        assertThatCode(() -> factory.close("init-data")).doesNotThrowAnyException();
    }
}
