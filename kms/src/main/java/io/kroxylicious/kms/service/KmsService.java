/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Service interface for KMSs
 * @param <C> The config type
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
public interface KmsService<C, K, E> {
    @NonNull
    Kms<K, E> buildKms(C options);

}
