/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import io.kroxylicious.kms.service.KekId;
import io.kroxylicious.kms.service.KmsException;

public class StringKekid implements KekId {
    private final String keyId;

    public StringKekid(String keyId) {
        this.keyId = keyId;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K> K getId(Class<K> keyType) {
        if (!keyType.isAssignableFrom(String.class)) {
            throw new KmsException("Unsupported keyType (" + keyType + ") requested. Only Strings are supported");
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

        StringKekid that = (StringKekid) o;

        return keyId.equals(that.keyId);
    }

    @Override
    public int hashCode() {
        return keyId.hashCode();
    }
}