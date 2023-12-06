/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import io.kroxylicious.filter.encryption.EncryptionException;

/**
 * Identifies a particular cipher, and may include values for some or all of its parameters.
 */
public enum CipherCode {
    AES_GCM_96_128((byte) 0);

    private final byte code;

    CipherCode(byte code) {
        this.code = code;
    }

    public static CipherCode fromCode(byte code) {
        switch (code) {
            case 0:
                return AES_GCM_96_128;
            default:
                throw new EncryptionException("Unknown Cipher code");
        }
    }

    public byte code() {
        return code;
    }
}
