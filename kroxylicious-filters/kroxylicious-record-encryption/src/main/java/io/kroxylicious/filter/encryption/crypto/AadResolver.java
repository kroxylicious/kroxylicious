/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.util.Objects;

import io.kroxylicious.filter.encryption.common.EncryptionException;
import io.kroxylicious.filter.encryption.config.AadSpec;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface AadResolver {
    AadResolver INSTANCE = new AadResolver() {
        public byte persistentId(@NonNull Aad aad) {
            Objects.requireNonNull(aad);
            if (aad instanceof AadNone) {
                return 0;
            }
            else {
                throw new EncryptionException("Unknown AAD " + aad.getClass());
            }
        }

        public Aad fromSpec(AadSpec spec) {
            Objects.requireNonNull(spec);
            switch (spec) {
                case NONE -> {
                    return new AadNone();
                }
            }
            throw new EncryptionException("Unknown AAD " + spec);
        }

        public Aad fromPersistentId(byte persistentId) {
            switch (persistentId) {
                case 0:
                    return new AadNone();
                default:
                    throw new EncryptionException("Unknown AAD persistent id " + persistentId);
            }
        }
    };

    byte persistentId(@NonNull Aad aad);

    Aad fromPersistentId(byte persistentId);
}
