/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BadTlsCredentialsExceptionTest {

    @Test
    void messageConstructorSetsMessage() {
        BadTlsCredentialsException exception = new BadTlsCredentialsException("bad credentials");

        assertThat(exception)
                .hasMessage("bad credentials")
                .hasNoCause();
    }

    @Test
    void messageAndCauseConstructorSetsMessageAndCause() {
        RuntimeException cause = new RuntimeException("cause");

        BadTlsCredentialsException exception = new BadTlsCredentialsException("bad credentials", cause);

        assertThat(exception)
                .hasMessage("bad credentials")
                .hasCause(cause);
    }
}
