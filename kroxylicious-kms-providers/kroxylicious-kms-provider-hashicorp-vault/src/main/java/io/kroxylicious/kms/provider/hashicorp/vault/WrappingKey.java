/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.util.Objects;

/**
 * A reference to a HashiCorp Vault Transit wrapping key (KEK), combining the stable key name
 * with the version number from the most recent {@code resolveAlias} call.
 *
 * <p>In Vault Transit, rotating a key appends a new version to the same named key ring — the name
 * never changes. {@code WrappingKey} captures both pieces of information so that the resolved value
 * can serve as a cache-invalidation token: when a rotation bumps {@code version}, the resolved value
 * is no longer {@code equals()} to the previously cached one. Downstream caches (e.g.
 * {@code EncryptionDekCache}) key on the full {@code WrappingKey}, so a changed version causes a
 * cache miss and forces a fresh DEK to be generated under the new key version.
 *
 * <p>The version is <em>not</em> sent to Vault on the encrypt path. {@code generateDekPair} posts to
 * {@code transit/datakey/plaintext/{name}} without a {@code key_version} field, which causes Vault to
 * use the latest version automatically. There is therefore a benign race: if a rotation lands between
 * {@code resolveAlias} and {@code generateDekPair}, Vault may encrypt with a version newer than the one
 * recorded here. This is acceptable — the EDEK is self-describing and decryption always succeeds.
 *
 * @param name    the stable name of the key ring in Vault Transit (unchanged across rotations)
 * @param version the latest key version as returned by the Vault read-key API ({@code latest_version});
 *                changes on each rotation and is used only for cache invalidation
 */
public record WrappingKey(String name, int version) {

    /**
     * Creates a wrapping key reference.
     */
    public WrappingKey {
        Objects.requireNonNull(name, "name must not be null");
    }
}
