/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

/**
 * An in-memory KMS to be used only for testing.
 * Note that this exposes public methods that are not part of the {@link Kms} interface which are used for those
 * KMS operations which are outside the scope of Kroxylicious itself (such as key provisioning).
 */
public class InMemoryKms implements
        Kms<UUID, InMemoryEdek> {

    private static final String AES_WRAP_ALGO = "AES_256/GCM/NoPadding";
    public static final String AES_KEY_ALGO = "AES";
    private final Map<UUID, DestroyableRawSecretKey> keys;
    private final KeyGenerator aes;
    private final int numIvBytes;
    private final int numAuthBits;
    private final SecureRandom secureRandom;
    private final Map<String, UUID> aliases;
    private final List<DekPair<InMemoryEdek>> edeksGenerated = new CopyOnWriteArrayList<>();

    InMemoryKms(int numIvBytes,
                int numAuthBits,
                Map<UUID, DestroyableRawSecretKey> keys,
                Map<String, UUID> aliases) {
        this.keys = new ConcurrentHashMap<>(keys);
        this.aliases = new ConcurrentHashMap<>(aliases);
        this.secureRandom = new SecureRandom();
        this.numIvBytes = numIvBytes;
        this.numAuthBits = numAuthBits;
        try {
            this.aes = KeyGenerator.getInstance(AES_KEY_ALGO);
            this.aes.init(256); // Required for Java 17 which defaults to a key size of 128.
        }
        catch (NoSuchAlgorithmException e) {
            // This should be impossible, because JCA guarantees that AES is available
            throw new KmsException(e);
        }
    }

    /**
     * Generates a KEK
     * @return The id of the KEK
     */
    public UUID generateKey() {
        var key = DestroyableRawSecretKey.toDestroyableKey(aes.generateKey());
        var ref = UUID.randomUUID();
        keys.put(ref, key);
        return ref;
    }

    public void createAlias(UUID keyId, String alias) {
        lookupKey(keyId); // check the key exists in this KMS
        aliases.put(alias, keyId);
    }

    public void deleteAlias(String alias) {
        var was = aliases.remove(alias);
        if (was == null) {
            throw new UnknownAliasException(alias);
        }
    }

    /**
     * @return the number of DEKs that have been generated.
     */
    public int numDeksGenerated() {
        return edeksGenerated.size();
    }

    private InMemoryEdek wrap(UUID kekRef, Supplier<SecretKey> generator) {
        SecretKey kek = lookupKey(kekRef);
        Cipher aesCipher = aesGcm();
        GCMParameterSpec spec = initializeForWrap(kek, aesCipher);
        var dek = generator.get();
        byte[] edek;
        try {
            edek = aesCipher.wrap(dek);
        }
        catch (IllegalBlockSizeException | InvalidKeyException e) {
            throw new KmsException(e);
        }
        return new InMemoryEdek(spec.getTLen(), spec.getIV(), kekRef, edek);
    }

    @Override
    public CompletableFuture<DekPair<InMemoryEdek>> generateDekPair(UUID kekRef) {
        try {
            var dek = DestroyableRawSecretKey.toDestroyableKey(this.aes.generateKey());
            var edek = wrap(kekRef, () -> dek);
            DekPair<InMemoryEdek> dekPair = new DekPair<>(edek, dek);
            edeksGenerated.add(dekPair);
            return CompletableFuture.completedFuture(dekPair);
        }
        catch (KmsException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private GCMParameterSpec initializeForWrap(SecretKey kek, Cipher aes) {
        byte[] iv = new byte[numIvBytes];
        secureRandom.nextBytes(iv);
        var spec = new GCMParameterSpec(numAuthBits, iv);
        try {
            aes.init(Cipher.WRAP_MODE, kek, spec);
        }
        catch (GeneralSecurityException e) {
            throw new KmsException(e);
        }
        return spec;
    }

    private DestroyableRawSecretKey lookupKey(UUID kekRef) {
        DestroyableRawSecretKey kek = this.keys.get(kekRef);
        if (kek == null) {
            throw new UnknownKeyException();
        }
        return kek;
    }

    public void deleteKey(UUID kekRef) {
        if (aliases.containsValue(kekRef)) {
            throw new KmsException("key " + kekRef + " is referenced by an alias");
        }
        if (this.keys.remove(kekRef) == null) {
            throw new UnknownKeyException();
        }
    }

    @Override
    public CompletableFuture<SecretKey> decryptEdek(InMemoryEdek edek) {
        try {
            var kek = lookupKey(edek.kekRef());
            Cipher aesCipher = aesGcm();
            initializeforUnwrap(aesCipher, edek, kek);
            DestroyableRawSecretKey key = unwrap(edek, aesCipher);
            return CompletableFuture.completedFuture(key);
        }
        catch (KmsException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static DestroyableRawSecretKey unwrap(InMemoryEdek edek, Cipher aesCipher) {
        try {
            return DestroyableRawSecretKey.toDestroyableKey((SecretKey) aesCipher.unwrap(edek.edek(), AES_KEY_ALGO, Cipher.SECRET_KEY));
        }
        catch (GeneralSecurityException e) {
            throw new KmsException("Error unwrapping DEK", e);
        }
    }

    private static void initializeforUnwrap(Cipher aesCipher, InMemoryEdek edek, SecretKey kek) {
        var spec = new GCMParameterSpec(edek.numAuthBits(), edek.iv());
        try {
            aesCipher.init(Cipher.UNWRAP_MODE, kek, spec);
        }
        catch (GeneralSecurityException e) {
            throw new KmsException("Error initializing cipher", e);
        }
    }

    private static Cipher aesGcm() {
        try {
            return Cipher.getInstance(AES_WRAP_ALGO);
        }
        catch (GeneralSecurityException e) {
            throw new KmsException(e);
        }
    }

    @Override
    public CompletableFuture<UUID> resolveAlias(String alias) {
        UUID uuid = aliases.get(alias);
        if (uuid == null) {
            return CompletableFuture.failedFuture(new UnknownAliasException(alias));
        }
        return CompletableFuture.completedFuture(uuid);
    }

    @Override
    public Serde<InMemoryEdek> edekSerde() {
        return InMemoryEdekSerde.instance();
    }

    public DekPair<InMemoryEdek> getGeneratedEdek(int i) {
        return edeksGenerated.get(i);
    }

    public Class<InMemoryEdek> edekClass() {
        return InMemoryEdek.class;
    }
}
