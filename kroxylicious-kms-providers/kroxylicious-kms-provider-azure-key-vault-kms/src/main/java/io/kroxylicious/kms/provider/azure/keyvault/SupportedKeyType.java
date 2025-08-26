/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum SupportedKeyType {
    // supported by Key Vault Standard, Key Vault Premium SKU and Managed HSM
    RSA("RSA", "RSA-OAEP-256"),
    // supported by Key Vault Premium SKU and Managed HSM
    RSA_HSM("RSA-HSM", "RSA-OAEP-256"),
    // supported by Managed HSM
    OCT("oct", "A256GCM"),
    // supported by Managed HSM
    OCT_HSM("oct-HSM", "A256GCM");

    private static final Map<String, SupportedKeyType> API_KT_TO_SUPPORTED_KT = Arrays.stream(SupportedKeyType.values())
            .collect(Collectors.toMap(SupportedKeyType::getKeyType, Function.identity()));

    private final String keyType;
    private final String wrapAlgorithm;

    /**
     *
     * @param keyType the Key Type string from the API
     * @param wrapAlgorithm the wrapping algorithm to be used for this Key Type
     */
    SupportedKeyType(String keyType, String wrapAlgorithm) {
        this.keyType = keyType;
        this.wrapAlgorithm = wrapAlgorithm;
    }

    public static Optional<SupportedKeyType> fromKeyType(String keyType) {
        return Optional.ofNullable(API_KT_TO_SUPPORTED_KT.get(keyType));
    }

    public String getWrapAlgorithm() {
        return wrapAlgorithm;
    }

    public String getKeyType() {
        return keyType;
    }
}
