/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

class PlatformTrustProviderTest {
    @Test
    void testAccept() {
        TrustProvider trustProvider = PlatformTrustProvider.INSTANCE;
        assertThatCode(() -> trustProvider.accept(new TrustProviderVisitor<Void>() {
            @Override
            public Void visit(TrustStore trustStore) {
                throw new RuntimeException("unexpected call to visit(TrustStore)");
            }

            @Override
            public Void visit(InsecureTls insecureTls) {
                throw new RuntimeException("unexpected call to visit(InsecureTls)");
            }

            @Override
            public Void visit(PlatformTrustProvider platformTrustProviderTls) {
                return null;
            }

        })).doesNotThrowAnyException();
    }
}