/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.util.Collection;
import java.util.List;

import io.kroxylicious.filter.encryption.common.AbstractResolver;
import io.kroxylicious.filter.encryption.config.EncryptionVersion;

/**
 * A resolver of {@link EncryptionVersion}s to their corresponding {@link Encryption}s.
 */
public class EncryptionResolver extends AbstractResolver<EncryptionVersion, Encryption, EncryptionResolver> {

    /** A resolver of all the known {@link Encryption}s. */
    public static final EncryptionResolver ALL = new EncryptionResolver(List.of(Encryption.V1, Encryption.V2));

    EncryptionResolver(Collection<Encryption> impls) {
        super(impls);
    }

    @Override
    protected EncryptionResolver newInstance(Collection<Encryption> values) {
        return new EncryptionResolver(values);
    }

}