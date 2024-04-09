/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.util.Objects;

import io.kroxylicious.filter.encryption.config.CipherSpec;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface CipherSpecResolver {

    CipherSpecResolver INSTANCE = new CipherSpecResolver() {
        @Override
        public CipherManager fromPersistentId(int persistentId) {
            switch (persistentId) {
                case 0:
                    return Aes.AES_256_GCM_128;
                case 1:
                    return ChaChaPoly.INSTANCE;
                default:
                    throw new UnknownCipherSpecException("Cipher spec with persistent id " + persistentId + " is not known");
            }
        }

        @Override
        public byte persistentId(@NonNull CipherManager manager) {
            Objects.requireNonNull(manager);
            if (manager instanceof Aes) {
                return 0;
            }
            else if (manager instanceof ChaChaPoly) {
                return 1;
            }
            throw new UnknownCipherSpecException("Unknown CipherManager " + manager.getClass());
        }

        @Override
        public CipherManager fromSpec(CipherSpec spec) {
            switch (spec) {
                case AES_256_GCM_128:
                    return Aes.AES_256_GCM_128;
                case CHACHA20_POLY1305:
                    return ChaChaPoly.INSTANCE;
                default:
                    throw new UnknownCipherSpecException("Cipher spec " + spec + " is not known");
            }
        }
    };

    CipherManager fromPersistentId(int persistentId);

    byte persistentId(@NonNull CipherManager manager);

    CipherManager fromSpec(CipherSpec spec);

}
