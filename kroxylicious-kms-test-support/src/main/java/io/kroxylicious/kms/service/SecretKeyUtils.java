/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.security.MessageDigest;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;

public class SecretKeyUtils {

    private SecretKeyUtils() {
    }

    /**
     * Tests whether the arguments represent the same key.
     * @param thisKey The one key
     * @param thatKey The other key
     * @return true if they keys have the same algorithm and key material.
     */
    public static boolean same(
            @NonNull
            DestroyableRawSecretKey thisKey,
            @NonNull
            DestroyableRawSecretKey thatKey
    ) {
        if (thisKey == thatKey) {
            return true;
        }
        DestroyableRawSecretKey.checkNotDestroyed(Objects.requireNonNull(thisKey));
        DestroyableRawSecretKey.checkNotDestroyed(Objects.requireNonNull(thatKey));
        return thisKey.getAlgorithm().equals(thatKey.getAlgorithm())
               && MessageDigest.isEqual(thisKey.getEncoded(), thatKey.getEncoded()); // note: constant time impl
    }
}
