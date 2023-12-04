/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

/**
 * A semantic wrapper around KMS specific key id type
 * @param <K> the type of the underlying Key
 */
public interface KekId<K> {

    /**
     * Obtain the underlying key id
     * @return the underlying key ID
     */
    K getId();
}