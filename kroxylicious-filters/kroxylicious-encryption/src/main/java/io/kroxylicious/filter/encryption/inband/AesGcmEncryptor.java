/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Objects;

import javax.annotation.concurrent.NotThreadSafe;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import io.kroxylicious.filter.encryption.EncryptionException;

import edu.umd.cs.findbugs.annotations.NonNull;

@NotThreadSafe
class AesGcmEncryptor implements Destroyable {

    private int lastCall;
    private static final int OUTPUT_SIZE = 0;
    private static final int ENCRYPT = 1;
    private static final int DECRYPT = 2;

    private SecretKey key;
    private Cipher cipher;
    private final int numAuthBits;
    private final byte[] iv;
    private final AesGcmIvGenerator ivGenerator;

    private AesGcmEncryptor(int ivSizeBytes, AesGcmIvGenerator ivGenerator, @NonNull SecretKey key) {
        // NIST SP.800-38D recommends 96 bit for recommendation about the iv length and generation
        this.iv = new byte[ivSizeBytes];
        this.ivGenerator = ivGenerator;
        this.numAuthBits = 128;
        this.key = Objects.requireNonNull(key);
        try {
            this.cipher = Cipher.getInstance("AES_256/GCM/NoPadding");
        }
        catch (GeneralSecurityException e) {
            throw new EncryptionException(e);
        }
    }

    public static AesGcmEncryptor forEncrypt(@NonNull AesGcmIvGenerator ivGenerator, @NonNull SecretKey key) {
        Objects.requireNonNull(ivGenerator);
        var result = new AesGcmEncryptor(ivGenerator.sizeBytes(), ivGenerator, key);
        result.lastCall = ENCRYPT;
        return result;
    }

    @NonNull
    public static AesGcmEncryptor forDecrypt(@NonNull SecretKey key) {
        var result = new AesGcmEncryptor(12, null, key);
        result.init(Cipher.DECRYPT_MODE);
        result.lastCall = DECRYPT;
        return result;

    }

    public int outputSize(int plaintextSize) {
        checkNotDestroyed();
        ivGenerator.generateIv(iv);
        init(Cipher.ENCRYPT_MODE);
        int result = headerSize() // iv
                + this.cipher.getOutputSize(plaintextSize);
        lastCall = OUTPUT_SIZE;
        return result;
    }

    private int headerSize() {
        return Byte.BYTES // version
                + Byte.BYTES // iv length
                + ivGenerator.sizeBytes();
    }

    /**
     * Encrypt the given plaintext, writing the ciphertext and any necessary extra data to the given {@code output}.
     * @param plaintext The plaintext to encrypt.
     * @param output The output buffer.
     */
    public void encrypt(@NonNull ByteBuffer plaintext, @NonNull ByteBuffer output) {
        checkNotDestroyed();
        if (lastCall != OUTPUT_SIZE) {
            throw new IllegalStateException("Call to encrypt() without last call being to outputSize()");
        }
        lastCall = ENCRYPT;
        int p0 = output.position();
        output.position(p0 + headerSize());
        try {
            this.cipher.doFinal(plaintext, output);
        }
        catch (GeneralSecurityException e) {
            throw new EncryptionException(e);
        }
        int p1 = output.position();
        output.position(p0);
        byte version = 0;
        output.put(version);
        output.put((byte) iv.length);
        output.put(iv); // the iv
        output.position(p1);
    }

    private void init(int encryptMode) {
        var spec = new GCMParameterSpec(numAuthBits, iv);
        try {
            this.cipher.init(encryptMode, key, spec);
        }
        catch (GeneralSecurityException e) {
            throw new EncryptionException(e);
        }
    }

    public void decrypt(@NonNull ByteBuffer input, @NonNull ByteBuffer plaintext) {
        checkNotDestroyed();
        if (lastCall != DECRYPT) {
            throw new IllegalStateException();
        }
        var version = input.get();
        if (version == 0) {
            int ivLength = input.get();
            if (ivLength != iv.length) {
                throw new EncryptionException("Unexpected IV length");
            }
            input.get(iv, 0, iv.length);
            init(Cipher.DECRYPT_MODE);
            try {
                this.cipher.doFinal(input, plaintext);
            }
            catch (GeneralSecurityException e) {
                throw new EncryptionException(e);
            }
        }
        else {
            throw new EncryptionException("Unknown version " + version);
        }
    }

    private void checkNotDestroyed() {
        if (key == null) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void destroy() throws DestroyFailedException {
        ivGenerator.destroy();
        Arrays.fill(iv, (byte) 0);
        try {
            if (key != null) {
                key.destroy();
            }
        }
        catch (DestroyFailedException e) {
            throw new DestroyFailedException("On key of " + key.getClass());
        }
        finally {
            key = null;
            cipher = null;
        }
    }

    @Override
    public boolean isDestroyed() {
        return key == null;
    }
}
