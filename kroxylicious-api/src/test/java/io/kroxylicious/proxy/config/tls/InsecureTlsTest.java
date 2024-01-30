/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InsecureTlsTest {

    @Test
    void testAccept() {
        TrustProvider trustProvider = new InsecureTls(true);
        InsecureTls result = trustProvider.accept(new TrustProviderVisitor<>() {
            @Override
            public InsecureTls visit(TrustStore trustStore) {
                throw new RuntimeException("should not be called");
            }

            @Override
            public InsecureTls visit(InsecureTls insecureTls) {
                return insecureTls;
            }
        });
        assertThat(result).isSameAs(trustProvider);
    }
}