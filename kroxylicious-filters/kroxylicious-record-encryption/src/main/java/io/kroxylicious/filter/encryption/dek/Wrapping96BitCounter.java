/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.security.SecureRandom;

import javax.annotation.concurrent.NotThreadSafe;
import javax.security.auth.Destroyable;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A 96-bit counter with a random initial state which wraps around when
 * it overflows. The bits returned via {@link #generateIv(byte[])} will repeat after exactly
 * 2<sup>96</sup> invocations.
 *
 * This can function as a RBG-based construction of the Initialization Vector for AES-GCM.
 * The random field is 96 bits and the free field is empty.
 *
 * @see "§8.2.2 of NIST SP.800-38D: Recommendation for Block
 * Cipher Modes of Operation:
 * Galois/Counter Mode (GCM)
 * and GMAC"
 */
@NotThreadSafe
class Wrapping96BitCounter implements Destroyable {

    private int low;
    private int mid;
    private int hi;
    private boolean destroyed = false;

    Wrapping96BitCounter(@NonNull
    SecureRandom rng) {
        low = rng.nextInt();
        mid = rng.nextInt();
        hi = rng.nextInt();
    }

    @VisibleForTesting
    int sizeBytes() {
        return 12;
    }

    void generateIv(byte[] iv) {
        if (destroyed) {
            throw new IllegalStateException();
        }
        iv[0] = (byte) (hi >> 24);
        iv[1] = (byte) (hi >> 16);
        iv[2] = (byte) (hi >> 8);
        iv[3] = (byte) (hi);
        iv[4] = (byte) (mid >> 24);
        iv[5] = (byte) (mid >> 16);
        iv[6] = (byte) (mid >> 8);
        iv[7] = (byte) (mid);
        iv[8] = (byte) (low >> 24);
        iv[9] = (byte) (low >> 16);
        iv[10] = (byte) (low >> 8);
        iv[11] = (byte) (low);
        try {
            low = Math.addExact(low, 1);
        }
        catch (ArithmeticException e) {
            low = Integer.MIN_VALUE;
            try {
                mid = Math.addExact(mid, 1);
            }
            catch (ArithmeticException e2) {
                mid = Integer.MIN_VALUE;
                try {
                    hi = Math.addExact(hi, 1);
                }
                catch (ArithmeticException e3) {
                    hi = Integer.MIN_VALUE;
                }
            }
        }
    }

    @Override
    public void destroy() {
        low = 0;
        mid = 0;
        hi = 0;
        destroyed = true;
    }

    @Override
    public boolean isDestroyed() {
        return destroyed;
    }
}
