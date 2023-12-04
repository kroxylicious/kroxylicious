/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

/**
 * A semantic wrapper around KMS specific key id type
 */
public interface KekId {

    /**
     * Obtain the underlying key id
     *
     * @param <K> the type of the underlying Key
     *
     * @return the underlying key ID
     */
    <K> K getId(Class<K> keyType);
}