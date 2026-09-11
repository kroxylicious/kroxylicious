/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore;

/**
 * Hash algorithms supported for SCRAM authentication.
 */
public enum ScramHashAlgorithm {

    /**
     * SHA-256 hash algorithm, used with SCRAM-SHA-256.
     */
    SHA_256("SHA-256"),

    /**
     * SHA-512 hash algorithm, used with SCRAM-SHA-512.
     */
    SHA_512("SHA-512");

    private final String algorithmName;

    ScramHashAlgorithm(String algorithmName) {
        this.algorithmName = algorithmName;
    }

    /**
     * Returns the JCA standard algorithm name (e.g. {@code "SHA-256"}).
     *
     * @return the algorithm name
     */
    public String algorithmName() {
        return algorithmName;
    }

    /**
     * Returns the enum constant for the given JCA algorithm name.
     *
     * @param algorithmName the JCA algorithm name (e.g. {@code "SHA-256"})
     * @return the corresponding enum constant
     * @throws IllegalArgumentException if the algorithm name is not supported
     */
    public static ScramHashAlgorithm fromAlgorithmName(String algorithmName) {
        for (ScramHashAlgorithm algorithm : values()) {
            if (algorithm.algorithmName.equals(algorithmName)) {
                return algorithm;
            }
        }
        throw new IllegalArgumentException("Unsupported hash algorithm: " + algorithmName);
    }
}
