/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

/**
 * Enumerates the sets of metadata which can be used as additional authenticated date for AEAD ciphers.
 * Each element in this enumeration corresponds to a schema for the serialization of that set of metadata.
 */
public enum Aad {
    /**
     * No AAD
     */
    NONE((byte) 0, EncryptionVersion.V1);
    // TODO we need to constraint which Aad options are allowed in which EncrytionVersions
//    /**
//     * AAD consisting of the batch metadata plus the records position within the batch.
//     */
//    BATCH_METADATA(1);

    private final byte code;
    private final EncryptionVersion fromVersion;

    Aad(byte code, EncryptionVersion fromVersion) {
        this.code = code;
        this.fromVersion = fromVersion;
    }

    public void check(EncryptionVersion encryptionVersion) {
        if (encryptionVersion.compareTo(fromVersion) < 0) {
            throw new EncryptionConfigurationException("AAD " + this + " only supported from encryption version " + fromVersion);
        }
    }

    public byte code() {
        return code;
    }

    public static Aad fromCode(byte aadCode) {
        switch (aadCode) {
            case 0:
                return NONE;
            default:
                throw new EncryptionException("Unknown AAD code " + aadCode);
        }
    }
}
