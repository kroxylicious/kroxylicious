/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import java.util.Optional;

public enum SupportedKeyType {
    RSA("RSA-OAEP-256", false),
    AES("A256GCM", true);

    private final String wrapAlgorithm;
    private final boolean quantumResistant;

    SupportedKeyType(String wrapAlgorithm, boolean quantumResistant) {
        this.wrapAlgorithm = wrapAlgorithm;
        this.quantumResistant = quantumResistant;
    }

    public static Optional<SupportedKeyType> fromKeyType(String keyType) {
        return switch (keyType) {
            case "RSA", "RSA-HSM" -> Optional.of(RSA);
            case "oct", "oct-HSM" -> Optional.of(AES);
            default -> Optional.empty();
        };
    }

    public String getWrapAlgorithm() {
        return wrapAlgorithm;
    }

    public boolean isQuantumResistant() {
        return quantumResistant;
    }

}
