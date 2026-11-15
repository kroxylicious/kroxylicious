/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

/**
 * Test-only constants for port numbers used in integration tests.
 * These mirror the port allocation logic in NetworkingPlanner but are kept separate
 * to avoid coupling production code to test expectations.
 */
public final class TestPortConstants {

    /**
     * Shared SNI port used when PROXY protocol mode is REQUIRED (9290).
     */
    public static final int PROXY_PROTOCOL_SNI_PORT = 9290;

    /**
     * Shared SNI port used when PROXY protocol mode is FORBIDDEN or unset (9291).
     */
    public static final int PLAIN_SNI_PORT = 9291;

    /**
     * First port in the range used for per-broker identifying ports (9292).
     * Subsequent brokers use 9293, 9294, etc.
     */
    public static final int FIRST_IDENTIFYING_PORT = 9292;

    private TestPortConstants() {
        // Utility class
    }
}
