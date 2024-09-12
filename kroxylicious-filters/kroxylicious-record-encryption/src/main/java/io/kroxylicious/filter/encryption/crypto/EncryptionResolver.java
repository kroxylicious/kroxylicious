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

public class EncryptionResolver extends AbstractResolver<EncryptionVersion, Encryption, EncryptionResolver> {

    public static final EncryptionResolver ALL = new EncryptionResolver(List.of(Encryption.V1, Encryption.V2));

    EncryptionResolver(Collection<Encryption> impls) {
        super(impls);
    }

    @Override
    protected EncryptionResolver newInstance(Collection<Encryption> values) {
        return new EncryptionResolver(values);
    }

}
