/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.policy;

public enum TopicEncryptionPolicy {

    LEGACY {
        @Override
        public boolean shouldAttemptKeyResolution() {
            return true;
        }
    };

    public abstract boolean shouldAttemptKeyResolution();

}
