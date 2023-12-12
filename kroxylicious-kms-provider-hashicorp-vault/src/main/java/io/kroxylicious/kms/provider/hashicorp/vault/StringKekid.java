/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import io.kroxylicious.kms.service.KekId;
import io.kroxylicious.kms.service.KmsException;

public record StringKekid(String keyId) implements KekId {

    @SuppressWarnings("unchecked")
    @Override
    public <K> K getId(Class<K> keyType) {
        if (!keyType.isAssignableFrom(String.class)) {
            throw new KmsException("Unsupported keyType (" + keyType + ") requested. Only Strings are supported");
        }
        return (K) this.keyId;
    }

}