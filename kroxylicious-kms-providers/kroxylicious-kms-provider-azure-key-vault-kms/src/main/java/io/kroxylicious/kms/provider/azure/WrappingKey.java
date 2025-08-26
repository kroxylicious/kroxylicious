/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.kroxylicious.kms.provider.azure.keyvault.SupportedKeyType;

public record WrappingKey(String keyName, String keyVersion, SupportedKeyType supportedKeyType, String vaultName) {

    public WrappingKey {
        Objects.requireNonNull(keyName, "keyName is null");
        Objects.requireNonNull(keyVersion, "keyVersion is null");
        Objects.requireNonNull(supportedKeyType, "supportedKeyType is null");
        Objects.requireNonNull(vaultName, "vaultName is null");
    }

    /**
     * Parse WrappingKey details from the keyId which is an Object Identifier.
     * <pre>
     *     Objects are uniquely identified within Key Vault using a case-insensitive identifier called
     *     the object identifier. No two objects in the system have the same identifier, regardless of
     *     geo-location. The identifier consists of a prefix that identifies the key vault, object type,
     *     user provided object name, and an object version. Identifiers that don't include the object
     *     version are referred to as "base identifiers". Key Vault object identifiers are also valid
     *     URLs, but should always be compared as case-insensitive strings.
     *
     *     An object identifier has the following general format (depending on container type):
     *     - For Vaults: https://{vault-name}.vault.azure.net/{object-type}/{object-name}/{object-version}
     *     - For Managed HSM pools: https://{hsm-name}.managedhsm.azure.net/{object-type}/{object-name}/{object-version}
     * </pre>
     * For example:{@code https://myvault.vault.azure.net/keys/CreateSoftKeyTest/78deebed173b48e48f55abf87ed4cf71}
     * <a href="https://learn.microsoft.com/en-us/azure/key-vault/general/about-keys-secrets-certificates#object-identifiers">See Also</a>
     *
     * @param vaultName name of the vault
     * @param keyName name of the key
     * @param keyId key id in Object identifier form
     * @param supportedKeyType supported key type
     * @return WrappingKey
     * @throws IllegalArgumentException if keyId cannot be parsed as a URI or we cannot extract the object-version from it
     */
    public static WrappingKey parse(String vaultName, String keyName, String keyId, SupportedKeyType supportedKeyType) {
        Objects.requireNonNull(keyName, "keyName is null");
        Objects.requireNonNull(keyId, "keyId is null");
        Objects.requireNonNull(vaultName, "vaultName is null");
        if (vaultName.isBlank()) {
            throw new IllegalArgumentException("vaultName is blank");
        }
        URI uri = parseUri(keyId);
        String keyVersion = extractKeyVersion(uri);
        return new WrappingKey(keyName, keyVersion, supportedKeyType, vaultName);
    }

    private static String extractKeyVersion(URI uri) {
        String path = uri.getPath();
        validatePath(path);
        List<String> pathSegments = splitPath(path);
        return pathSegments.get(pathSegments.size() - 1);
    }

    public static List<String> splitPath(String pathString) {
        return Arrays.stream(pathString.split("/"))
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private static URI parseUri(String keyId) {
        try {
            return URI.create(keyId);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("failed to parse keyId '" + keyId + "' as a URI", e);
        }
    }

    private static void validatePath(String path) {
        if (path.endsWith("/")) {
            throw new IllegalArgumentException("keyId path '" + path + "' must not end with '/'");
        }
        if (!path.contains("/")) {
            throw new IllegalArgumentException("keyId path '" + path + "' must contain a '/'");
        }
    }
}
