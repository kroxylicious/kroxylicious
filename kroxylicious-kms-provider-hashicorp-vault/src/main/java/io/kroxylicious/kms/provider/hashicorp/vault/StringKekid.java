/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.util.Objects;

import io.kroxylicious.kms.service.KekId;
import io.kroxylicious.kms.service.KmsException;

import edu.umd.cs.findbugs.annotations.NonNull;

public record StringKekid(@NonNull String keyRef) implements KekId {

    public StringKekid {
        Objects.requireNonNull(keyRef);
        if (keyRef.isEmpty()) {
            throw new IllegalArgumentException("keyRef cannot be empty");
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    public <K> K getId(Class<K> keyType) {
        if (!keyType.isAssignableFrom(String.class)) {
            throw new KmsException("Unsupported keyType (" + keyType + ") requested. Only Strings are supported");
        }
        return (K) this.keyRef;
    }

}
