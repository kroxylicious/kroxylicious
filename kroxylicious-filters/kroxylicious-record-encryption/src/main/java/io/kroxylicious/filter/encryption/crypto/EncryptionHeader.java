/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

public class EncryptionHeader {

    /**
     * The encryption header. The value is the encryption version that was used to serialize the parcel and the wrapper.
     */
    public static final String ENCRYPTION_HEADER_NAME = "kroxylicious.io/encryption";

    private EncryptionHeader() {

    }
}
