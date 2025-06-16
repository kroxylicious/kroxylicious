/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

/**
 * Test key material.  Not for production use.
 */
public final class TestKeyMaterial {

    private TestKeyMaterial() {
        // singleton
    }

    /**
     * this constant and {@link #TEST_CERT_PEM} were generated using the following command:
     * {@code }openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -sha256 -days 3650 -nodes -subj "/CN=localhost"}
     */
    @SuppressWarnings("secrets:S6706")
    public static final String TEST_KEY_PEM = """
            -----BEGIN PRIVATE KEY-----
            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
            /iCHr8a4Yi6reU+GgLHDneKMqF1SQHu2
            -----END PRIVATE KEY-----
            """;
    public static final String TEST_CERT_PEM = """
            -----BEGIN CERTIFICATE-----
            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
            -----END CERTIFICATE-----
            """;
}
