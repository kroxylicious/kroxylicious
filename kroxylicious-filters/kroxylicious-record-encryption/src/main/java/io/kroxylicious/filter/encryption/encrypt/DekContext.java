/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.encrypt;

import java.util.concurrent.CompletionStage;

import io.kroxylicious.filter.encryption.dek.Dek;

/**
 * The DEK is shared between threads to enable efficient usage of each Secret Key. We want to
 * use each key as much as possible to reduce the load on the KMS at encrypt and decrypt time
 * and generally make the most of our resources.
 * Sharing it across threads means that we have to co-ordinate certain operations. For example
 * to trigger the loading of a new DEK we may need to invalidate it out of a cache, but we must
 * take care not to trigger parallel invalidations that accidentally invalidate the next DEK.
 * This context is responsible for that co-ordination.
 * @param <E> The type of encrypted DEK.
 */
public interface DekContext<E> {
    /**
     * Destroy this DEK for encryption and initiate rotation, instructing other
     * components to prepare to create a new EDEK. A DekContext can only be rotated once
     * in it's life. This method is idempotent, further calls to `rotate` will not make
     * any changes and the same future will be returned.
     * @return a future that is completed after rotation, enabling multiple threads
     * to wait for rotation before retrying the encryption operation. If there are any
     * exceptions during rotation the future will be completed exceptionally.
     */
    CompletionStage<Void> rotate();

    /**
     * Get the DEK for this context
     */
    Dek<E> getDek();
}
