/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import io.kroxylicious.kms.provider.thales.ciphertrust.model.GetKeyResponse;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Versioned key store for CTM key versioning support.
 * Each key can have multiple versions, with version 0 being the initial version.
 * Also stores key metadata including name and labels.
 */
class VersionedKeyStore {
    private static SecretKey generateAesKey() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256);
            return keyGen.generateKey();
        }
        catch (Exception e) {
            throw new CipherTrustMockServer.MockServerException("Failed to generate AES key", e);
        }
    }

    /**
     * @param secretKey  Single key, not multiple versions
     * @param version  Version number for this key */
    record KeyMetadata(String id, String name, Map<String, String> labels, SecretKey secretKey, int version) {
        KeyMetadata(String id, String name, Map<String, String> labels, SecretKey secretKey, int version) {
            this.id = id;
            this.name = name;
            this.labels = new HashMap<>(labels);
            this.secretKey = secretKey;
            this.version = version;
        }
    }

    private final Map<String, KeyMetadata> keysById = new ConcurrentHashMap<>();

    /**
     * Create a new key at version 0 with metadata.
     */
    synchronized void createKey(String keyId, String name, Map<String, String> labels) {
        SecretKey secretKey = generateAesKey();
        keysById.put(keyId, new KeyMetadata(keyId, name, labels, secretKey, 0));
    }

    /**
     * Rotate a key - creates a NEW key with NEW ID, SAME name, incremented version.
     * @return the new key ID, or null if the old key doesn't exist
     */
    @Nullable
    synchronized String rotateKey(String keyId) {
        KeyMetadata oldKey = keysById.get(keyId);
        if (oldKey == null) {
            return null;
        }

        // Calculate new version: highest existing version for this name + 1
        int newVersion = keysById.values().stream()
                .filter(m -> m.name.equals(oldKey.name))
                .mapToInt(m -> m.version)
                .max()
                .orElse(-1) + 1;

        // Create completely NEW key with NEW ID, NEW secret, SAME name, higher version
        String newKeyId = UUID.randomUUID().toString();
        SecretKey newSecretKey = generateAesKey();
        KeyMetadata newKey = new KeyMetadata(newKeyId, oldKey.name, oldKey.labels, newSecretKey, newVersion);

        keysById.put(newKeyId, newKey);

        return newKeyId;
    }

    /**
     * Rotate a key by name - creates a NEW key with NEW ID, SAME name, incremented version.
     * @param name the key name
     * @return the new key ID, or null if no key with that name exists
     */
    @Nullable
    synchronized String rotateKeyByName(String name) {
        // Find the key with the highest version for this name
        KeyMetadata currentKey = keysById.values().stream()
                .filter(m -> m.name.equals(name))
                .max(Comparator.comparingInt(m -> m.version))
                .orElse(null);

        if (currentKey == null) {
            return null; // No key found with this name
        }

        // Delegate to existing rotateKey() method using the current key's ID
        return rotateKey(currentKey.id);
    }

    /**
     * Get the secret key for a given key ID.
     */
    @Nullable
    SecretKey getKey(String keyId) {
        KeyMetadata metadata = keysById.get(keyId);
        return metadata != null ? metadata.secretKey : null;
    }

    /**
     * Get key metadata by name (returns the highest version).
     * @param name the key name
     * @return the key metadata with the highest version, or null if not found
     */
    @Nullable
    KeyMetadata getKeyMetadataByName(String name) {
        return keysById.values().stream()
                .filter(m -> m.name.equals(name))
                .max(Comparator.comparingInt(m -> m.version))
                .orElse(null);
    }

    /**
     * Find keys by name.
     */
    List<GetKeyResponse> findByName(String name) {
        return keysById.values().stream()
                .filter(m -> m.name.equals(name))
                .map(this::toGetKeyResponse)
                .toList();
    }

    /**
     * Find the key with the highest version for a given name (handles rotation).
     * @return the key with highest version, or null if no key found
     */
    @Nullable
    GetKeyResponse findKeyByNameWithHighestVersion(String name) {
        return keysById.values().stream()
                .filter(m -> m.name.equals(name))
                .max(Comparator.comparingInt(m -> m.version))
                .map(this::toGetKeyResponse)
                .orElse(null);
    }

    /**
     * Find keys by label filter (format: key=value).
     */
    List<GetKeyResponse> findByLabels(String labelFilter) {
        // Parse label filter: key=value
        int eqIndex = labelFilter.indexOf('=');
        if (eqIndex < 0) {
            return List.of();
        }
        String labelKey = labelFilter.substring(0, eqIndex);
        String labelValue = labelFilter.substring(eqIndex + 1);

        return keysById.values().stream()
                .filter(m -> labelValue.equals(m.labels.get(labelKey)))
                .map(this::toGetKeyResponse)
                .toList();
    }

    /**
     * Find all keys.
     */
    List<GetKeyResponse> findAll() {
        return keysById.values().stream()
                .map(this::toGetKeyResponse)
                .toList();
    }

    /**
     * Delete a key and all its versions.
     */
    void deleteKey(String keyId) {
        keysById.remove(keyId);
    }

    private GetKeyResponse toGetKeyResponse(KeyMetadata metadata) {
        return new GetKeyResponse(metadata.id, metadata.name, "aes");
    }
}
