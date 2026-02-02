/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import io.kroxylicious.filter.encryption.common.EncryptionException;

/**
 * The encryption operation failed because the behaviour to apply to a topic (encrypt, passthrough)
 * could not be determined.
 */
public class UndeterminedTopicBehaviourException extends EncryptionException {
    public UndeterminedTopicBehaviourException(String message) {
        super(message);
    }
}
