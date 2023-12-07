/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

/**
 * <p>Identifies a particular cipher, and may include values for some or all of its parameters.</p>
 *
 * <p>The ciphers available to be used depends on the {@link EncryptionVersion}:
 * You cannot add a new cipher to an existing EncryptionVersion because doing
 * so would expose old versions of the software to ciphers that they don't know
 * about meaning they won't be able to supprt the decryption guarantee.</p>
 */
public enum CipherCode {
    AES_GCM_96_128((byte) 0, EncryptionVersion.V1);

    private final byte code;
    private final EncryptionVersion fromVersion;

    CipherCode(byte code, EncryptionVersion fromVersion) {
        this.code = code;
        this.fromVersion = fromVersion;
    }

    public void check(EncryptionVersion encryptionVersion) {
        if (encryptionVersion.compareTo(fromVersion) < 0) {
            throw new EncryptionConfigurationException("Cipher " + this + " only supported from encryption version " + fromVersion);
        }
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
