/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;
import javax.crypto.SecretKey;

/**
 * A SecretKey that has RAW encoding and can be {@linkplain #destroy() destroyed}
 * (unlike {@link javax.crypto.spec.SecretKeySpec}).
 */
@NotThreadSafe
public final class DestroyableRawSecretKey implements SecretKey {

    private final String algorithm;
    private boolean destroyed = false;
    private final byte[] key;

    public DestroyableRawSecretKey(String algorithm, byte[] bytes) {
        this.algorithm = algorithm;
        this.key = bytes.clone();
    }

    @Override
    public String getAlgorithm() {
        return algorithm;
    }

    @Override
    public String getFormat() {
        return "RAW";
    }

    /**
     * Returns the RAW-encoded key.
     * This is a copy the key. It is the callers responsibility to destroy this key material
     * when it's no longer needed.
     * @return The RAW-encoded key.
     */
    @Override
    public byte[] getEncoded() {
        checkNotDestroyed();
        return key.clone();
    }

    private void checkNotDestroyed() {
        if (isDestroyed()) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void destroy() {
        Arrays.fill(key, (byte) 0);
        destroyed = true;
    }

    @Override
    public boolean isDestroyed() {
        return destroyed;
    }
}
