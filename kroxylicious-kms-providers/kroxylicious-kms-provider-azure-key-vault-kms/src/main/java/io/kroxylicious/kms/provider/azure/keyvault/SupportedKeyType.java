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

/**
 * The Azure Key Vault key types supported by the KMS provider, along with the wrapping algorithm
 * used for each type.
 */
public enum SupportedKeyType {
    /**
     * A software-protected RSA key, supported by Key Vault Standard, Key Vault Premium SKU and Managed HSM.
     */
    RSA((byte) 0, "RSA", "RSA-OAEP-256"),
    /**
     * A hardware-protected RSA key, supported by Key Vault Premium SKU and Managed HSM.
     */
    RSA_HSM((byte) 1, "RSA-HSM", "RSA-OAEP-256"),
    /**
     * A software-protected symmetric key, supported by Managed HSM.
     */
    OCT((byte) 2, "oct", "A256GCM"),
    /**
     * A hardware-protected symmetric key, supported by Managed HSM.
     */
    OCT_HSM((byte) 3, "oct-HSM", "A256GCM");

    private static final Map<String, SupportedKeyType> API_KT_TO_SUPPORTED_KT = Arrays.stream(SupportedKeyType.values())
            .collect(Collectors.toMap(SupportedKeyType::getKeyType, Function.identity()));

    private static final Map<Byte, SupportedKeyType> ID_TO_SUPPORTED_KT = Arrays.stream(SupportedKeyType.values())
            .collect(Collectors.toMap(SupportedKeyType::getId, Function.identity()));
    private final String keyType;
    private final String wrapAlgorithm;
    private final byte id;

    /**
     *
     * @param keyType the Key Type string from the API
     * @param wrapAlgorithm the wrapping algorithm to be used for this Key Type
     */
    SupportedKeyType(byte id, String keyType, String wrapAlgorithm) {
        this.id = id;
        this.keyType = keyType;
        this.wrapAlgorithm = wrapAlgorithm;
    }

    /**
     * Looks up a supported key type from a Key Type string used by the Azure API.
     *
     * @param keyType the Key Type string from the API.
     * @return the supported key type, or empty if the key type is not supported.
     */
    public static Optional<SupportedKeyType> fromKeyType(String keyType) {
        return Optional.ofNullable(API_KT_TO_SUPPORTED_KT.get(keyType));
    }

    /**
     * Looks up a supported key type from the id used in the serialized form of an EDEK.
     *
     * @param id the id of the key type.
     * @return the supported key type, or empty if the id is unknown.
     */
    public static Optional<SupportedKeyType> fromId(byte id) {
        return Optional.ofNullable(ID_TO_SUPPORTED_KT.get(id));
    }

    /**
     * The wrapping algorithm to be used for this key type.
     *
     * @return the wrapping algorithm, e.g. {@code RSA-OAEP-256}.
     */
    public String getWrapAlgorithm() {
        return wrapAlgorithm;
    }

    /**
     * The Key Type string used by the Azure API for this key type.
     *
     * @return the Key Type string, e.g. {@code RSA}.
     */
    public String getKeyType() {
        return keyType;
    }

    /**
     * The id used to identify this key type in the serialized form of an EDEK.
     *
     * @return the id.
     */
    public byte getId() {
        return id;
    }

}
