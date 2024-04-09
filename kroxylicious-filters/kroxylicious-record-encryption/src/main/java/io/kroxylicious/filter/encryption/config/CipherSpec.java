/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

/**
 * Enumerates the configurable ciphers
 */
public enum CipherSpec {

    /**
     * AES/GCM with 256-bit key, 96-bit IV and 128-bit tag.
     * @see <a href="https://www.ietf.org/rfc/rfc5116.txt">RFC-5116</a>
     */
    AES_256_GCM_128,
    /**
     * ChaCha20-Poly1305, which means 256-bit key, 96-bit nonce and 128-bit tag.
     * @see <a href="https://www.ietf.org/rfc/rfc7539.txt">RFC-7539</a>
     */
    CHACHA20_POLY1305;
}
