/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import io.kroxylicious.kms.provider.azure.keyvault.SupportedKeyType;

public record AzureKeyVaultEdek(String keyName,
                                String keyVersion,
                                byte[] edek,
                                String vaultName,
                                SupportedKeyType supportedKeyType) {

    private static final Pattern KEY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9-]+$");

    public AzureKeyVaultEdek {
        Objects.requireNonNull(supportedKeyType, "supportedKeyType must not be null");
        Objects.requireNonNull(vaultName, "vaultName must not be null");
        if (vaultName.isBlank()) {
            throw new IllegalArgumentException("vaultName cannot be blank");
        }
        Objects.requireNonNull(keyName, "keyName must not be null");
        if (keyName.isBlank()) {
            throw new IllegalArgumentException("keyName cannot be blank");
        }
        if (keyName.length() > 127) {
            throw new IllegalArgumentException("keyName length cannot be longer than 127");
        }
        if (!KEY_NAME_PATTERN.matcher(keyName).matches()) {
            throw new IllegalArgumentException("keyName is not valid by pattern " + KEY_NAME_PATTERN.pattern());
        }
        Objects.requireNonNull(keyVersion, "keyVersion must not be null");
        if (keyVersion.isBlank()) {
            throw new IllegalArgumentException("keyVersion cannot be blank");
        }
        if (keyVersion.length() != 32) {
            throw new IllegalArgumentException("keyVersion must be 32 characters long");
        }
        Objects.requireNonNull(edek, "edek must not be null");
        if (edek.length == 0) {
            throw new IllegalArgumentException("edek cannot be empty");
        }
    }

    // we only tolerate lowercase to guarantee fidelity on deserialize
    private static boolean isLowercaseHex(String input) {
        if (input.isEmpty()) {
            return false;
        }
        return input.chars().allMatch(c -> (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
    }

    public Optional<byte[]> keyVersion128bit() {
        try {
            if (isLowercaseHex(keyVersion)) {
                byte[] bytes = HexFormat.of().parseHex(keyVersion);
                if (bytes.length != 16) {
                    // sanity check, should never happen because keyVersion must be 32 characters, so if it parses as hex we'll get 16 bytes
                    return Optional.empty();
                }
                else {
                    return Optional.of(bytes);
                }
            }
            else {
                return Optional.empty();
            }
        }
        catch (Exception e) {
            return Optional.empty();
        }
    }

    /**
     * Overridden to provide deep equality on the {@code byte[]}.
     * @param o   the reference object with which to compare.
     * @return true iff this object is equal to the given object.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AzureKeyVaultEdek that = (AzureKeyVaultEdek) o;
        return Objects.equals(keyName, that.keyName) && Objects.equals(vaultName, that.vaultName) && Objects.equals(supportedKeyType, that.supportedKeyType)
                && Objects.equals(keyVersion, that.keyVersion) && Arrays.equals(edek, that.edek);
    }

    /**
     * Overridden to provide a deep hashcode on the {@code byte[]}.
     * @return the has code.
     */
    @Override
    public int hashCode() {
        int result = Objects.hashCode(keyName);
        result = 31 * result + Objects.hashCode(vaultName);
        result = 31 * result + Objects.hashCode(supportedKeyType);
        result = 31 * result + Objects.hashCode(keyVersion);
        result = 31 * result + Arrays.hashCode(edek);
        return result;
    }

    /**
     * Overridden to provide a deep {@code toString()} on the {@code byte[]}.
     * @return The string
     */
    @Override
    public String toString() {
        return "AzureKeyVaultEdek{" +
                "keyName=" + keyName +
                ", vaultName=" + vaultName +
                ", supportedKeyType=" + supportedKeyType +
                ", keyVersion=" + keyVersion +
                ", edek=" + Arrays.toString(edek) +
                '}';
    }
}
