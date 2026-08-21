/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.util.Collection;
import java.util.List;

import io.kroxylicious.filter.encryption.common.AbstractResolver;
import io.kroxylicious.filter.encryption.config.CipherSpec;

/**
 * A resolver of {@link CipherSpec}s to their corresponding {@link CipherManager}s.
 */
public class CipherSpecResolver extends AbstractResolver<CipherSpec, CipherManager, CipherSpecResolver> {

    /** A resolver of all the known {@link CipherManager}s. */
    public static final CipherSpecResolver ALL = new CipherSpecResolver(List.of(
            Aes.AES_256_GCM_128,
            ChaChaPoly.INSTANCE));

    /**
     * Creates a resolver of the given cipher managers.
     * @param impls the cipher managers to resolve between.
     */
    public CipherSpecResolver(Collection<CipherManager> impls) {
        super(impls);
    }

    @Override
    protected RuntimeException newException(String msg) {
        return new UnknownCipherSpecException(msg);
    }

    /**
     * Creates a resolver of the cipher managers corresponding to the given cipher specs.
     * @param cipherSpec the cipher specs to resolve between.
     * @return a resolver of the cipher managers corresponding to the given cipher specs.
     */
    public static CipherSpecResolver of(CipherSpec... cipherSpec) {
        return ALL.subset(cipherSpec);
    }

    @Override
    protected CipherSpecResolver newInstance(Collection<CipherManager> values) {
        return new CipherSpecResolver(values);
    }
}
