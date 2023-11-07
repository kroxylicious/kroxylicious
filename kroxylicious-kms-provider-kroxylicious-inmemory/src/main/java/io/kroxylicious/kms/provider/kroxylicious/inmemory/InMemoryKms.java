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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An in-memory KMS to be used only for testing.
 * Note that this exposes public methods that are not part of the {@link Kms} interface which are used for those
 * KMS operations which are outside the scope of Kroxylicious itself (such as key provisioning).
 */
public class InMemoryKms implements
        Kms<UUID, InMemoryEdek> {

    private static final String AES_WRAP_ALGO = "AES_256/GCM/NoPadding";
    public static final String AES_KEY_ALGO = "AES";
    private final Map<UUID, SecretKey> keys;
    private final KeyGenerator aes;
    private final int numIvBytes;
    private final int numAuthBits;
    private final SecureRandom secureRandom;
    private final Map<String, UUID> aliases;

    private final AtomicInteger numDeksGenerated;

    InMemoryKms(int numIvBytes, int numAuthBits,
                Map<UUID, SecretKey> keys,
                Map<String, UUID> aliases,
                AtomicInteger numDeksGenerated) {
        this.keys = new HashMap<>(keys);
        this.aliases = new HashMap<>(aliases);
        this.numDeksGenerated = numDeksGenerated;
        this.secureRandom = new SecureRandom();
        this.numIvBytes = numIvBytes;
        this.numAuthBits = numAuthBits;
        try {
            this.aes = KeyGenerator.getInstance(AES_KEY_ALGO);
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
        var key = aes.generateKey();
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
        return numDeksGenerated.get();
    }

    @NonNull
    @Override
    public CompletableFuture<InMemoryEdek> generateDek(@NonNull UUID kekRef) {
        try {
            CompletableFuture<InMemoryEdek> result = CompletableFuture.completedFuture(wrap(kekRef, this.aes::generateKey));
            numDeksGenerated.incrementAndGet();
            return result;
        }
        catch (KmsException e) {
            return CompletableFuture.failedFuture(e);
        }
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
        return new InMemoryEdek(spec.getTLen(), spec.getIV(), edek);
    }

    @NonNull
    @Override
    public CompletableFuture<DekPair<InMemoryEdek>> generateDekPair(@NonNull UUID kekRef) {
        try {
            var dek = this.aes.generateKey();
            var edek = wrap(kekRef, () -> dek);
            numDeksGenerated.incrementAndGet();
            return CompletableFuture.completedFuture(new DekPair<>(edek, dek));
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

    private SecretKey lookupKey(UUID kekRef) {
        SecretKey kek = this.keys.get(kekRef);
        if (kek == null) {
            throw new UnknownKeyException();
        }
        return kek;
    }

    @NonNull
    @Override
    public CompletableFuture<SecretKey> decryptEdek(@NonNull UUID kekRef, @NonNull InMemoryEdek edek) {
        try {
            var kek = lookupKey(kekRef);
            Cipher aesCipher = aesGcm();
            initializeforUnwrap(aesCipher, edek, kek);
            SecretKey key = unwrap(edek, aesCipher);
            return CompletableFuture.completedFuture(key);
        }
        catch (KmsException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static SecretKey unwrap(@NonNull InMemoryEdek edek, Cipher aesCipher) {
        try {
            return (SecretKey) aesCipher.unwrap(edek.edek(), AES_KEY_ALGO, Cipher.SECRET_KEY);
        }
        catch (GeneralSecurityException e) {
            throw new KmsException("Error unwrapping DEK", e);
        }
    }

    private static void initializeforUnwrap(Cipher aesCipher, @NonNull InMemoryEdek edek, SecretKey kek) {
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

    @NonNull
    @Override
    public Serde<UUID> keyIdSerde() {
        return new UUIDSerde();
    }

    @NonNull
    @Override
    public CompletableFuture<UUID> resolveAlias(@NonNull String alias) {
        UUID uuid = aliases.get(alias);
        if (uuid == null) {
            return CompletableFuture.failedFuture(new UnknownAliasException(alias));
        }
        return CompletableFuture.completedFuture(uuid);
    }

    @NonNull
    @Override
    public Serde<InMemoryEdek> edekSerde() {
        return new InMemoryEdekSerde();
    }
}
