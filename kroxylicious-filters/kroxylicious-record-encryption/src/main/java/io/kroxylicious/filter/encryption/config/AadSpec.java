/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

/**
 * <p>Enumerates the sets of metadata which can be used as additional authenticated data (AAD) for AEAD ciphers.
 * Each element in this enumeration corresponds to a schema for the serialization of that set of metadata.
 * </p>
 *
 * <p>The AAD available to be used depends on the {@link EncryptionVersion}:
 * You cannot add a new cipher to an existing EncryptionVersion because doing
 * so would expose old versions of the software to ciphers that they don't know
 * about meaning they won't be able to supprt the decryption guarantee.</p>
 */
public enum AadSpec {
    /**
     * No AAD
     */
    NONE;

}
