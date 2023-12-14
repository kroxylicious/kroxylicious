/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import io.kroxylicious.filter.encryption.EncryptionException;

/**
 * Indicates a failure to obtain an Encryptor that can encrypt an entire batch.
 * <p>
 * A detail of the encryption algorithm is that we obtain an Encryptor object that
 * is only allowed to encrypt a certain number of records. If it is exhausted we
 * will try again, obtaining another encryptor and checking if it has encryptions
 * remaining. If we cannot obtain an encryptor with enough remaining encryptions
 * after some amount of retries this exception is thrown.
 * </p>
 */
public class EdekReservationFailureException extends EncryptionException {
    public EdekReservationFailureException(String message) {
        super(message);
    }
}
