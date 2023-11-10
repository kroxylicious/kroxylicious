/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import io.kroxylicious.filter.encryption.EncryptionException;

public class ExhaustedDekException extends EncryptionException {
    public ExhaustedDekException(String msg) {
        super(msg);
    }
}
