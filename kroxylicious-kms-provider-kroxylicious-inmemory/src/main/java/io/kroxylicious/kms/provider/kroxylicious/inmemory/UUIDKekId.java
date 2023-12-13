/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.Objects;
import java.util.UUID;

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.kms.service.KekId;
import io.kroxylicious.kms.service.KmsException;

public record UUIDKekId(@NonNull UUID keyId) implements KekId {

    public UUIDKekId {
        Objects.requireNonNull(keyId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K> K getId(Class<K> keyType) {
        if (!keyType.isAssignableFrom(UUID.class)) {
            throw new KmsException("Unsupported keyType (" + keyType + ") requested. Only UUID's are supported");
        }
        return (K) this.keyId;
    }
}