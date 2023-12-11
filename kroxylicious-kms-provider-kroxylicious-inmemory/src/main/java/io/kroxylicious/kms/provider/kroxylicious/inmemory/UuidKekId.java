/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.Objects;
import java.util.UUID;

import io.kroxylicious.kms.service.KekId;
import io.kroxylicious.kms.service.KmsException;

public record UuidKekId(UUID keyId) implements KekId {

    @SuppressWarnings("unchecked")
    @Override
    public <K> K getId(Class<K> keyType) {
        if (!keyType.isAssignableFrom(UUID.class)) {
            throw new KmsException("Unsupported keyType (" + keyType + ") requested. Only UUID's are supported");
        }
        return (K) this.keyId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UuidKekId uuidKekId = (UuidKekId) o;

        return Objects.equals(keyId, uuidKekId.keyId);
    }

    @Override
    public int hashCode() {
        return keyId != null ? keyId.hashCode() : 0;
    }
}