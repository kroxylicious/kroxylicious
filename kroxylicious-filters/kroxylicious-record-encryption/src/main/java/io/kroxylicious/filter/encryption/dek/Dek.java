/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;

import io.kroxylicious.kms.service.DestroyableRawSecretKey;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * <p>A Data Encryption Key (DEK) is an opaque handle on a key that can be used to encrypt and decrypt with some specific cipher.
 * The key itself is never accessible outside this class, only the ability to encrypt and decrypt is exposed.
 * </p>
 *
 * <h2>Encrypting and decrypting</h2>
 * <p>To encrypt using a DEK you need to obtain an {@link Encryptor Encryptor} using {@link #encryptor(int)},
 * specifying the number of encryption operations you expect to perform.
 * {@link Encryptor#generateParameters(EncryptAllocator) Encryptor.generateParameters()} and
 * {@link Encryptor#encrypt(ByteBuffer, ByteBuffer, EncryptAllocator) Encryptor.encrypt()}
 * can then be called up to the requested number of times.
 * Once you've finished using an Encryptor you need to {@link Encryptor#close() close()} it.
 * </p>
 * <p>Decryption works similarly, via {@link #decryptor()} and {@link Decryptor Decryptor},
 * except without a limit on the number of decryption operations</p>
 *
 * <h2>Thread safety</h2>
 * <p>{@code Dek} itself is designed to be thread-safe, however {@link Encryptor Encryptor} and {@link Decryptor Decryptor}
 * instances are not. {@link Encryptor Encryptor}s and {@link Decryptor Decryptor}s can be allocated from a common {@link Dek} instance that's shared between multiple threads,
 * but once allocated those cryptors should remain localised to a single thread for the duration of their lifetime.</p>
 *
 * <h2 id="destruction">DEK destruction</h2>
 * <p>A {@code Dek} can be destroyed, which will destroy
 * the underlying key material if possible.</p>
 * <p>To do this both {@link #destroyForEncrypt()} and {@link #destroyForDecrypt()}
 * need to be called on the key instance.
 * Even once both calls have been made the key is not destroyed immediately.
 * Instead calls to {@link #encryptor(int)} and {@link #decryptor()} will start to throw {@link DestroyedDekException},
 * but existing {@link Encryptor Encryptor} and {@link Decryptor Decryptor} instances will be able to continue.
 * The key will only be destroyed once the {@code close()} method has been called on all existing (en|de)cryptors.</p>
 *
 * @param <E> The type of encrypted DEK.
 */
@ThreadSafe
public final class Dek<E> {

    private final E edek;
    // Note: checks for outstandingCryptors==END <em>happens-before</em> changes to atomicKey.
    // It's this that provides a guarantee that a cryptor should never see a
    // destroyed or null key, but that keys should be destroyed and nullieifed as soon
    // as possible once all outstanding cryptors are closed.
    private final AtomicReference<DestroyableRawSecretKey> atomicKey;

    private final AtomicLong remainingEncryptions;

    final AtomicCryptorUsageCounter outstandingCryptors;
    private final CipherManager cipherManager;

    Dek(@NonNull E edek, @NonNull DestroyableRawSecretKey key, @NonNull CipherManager cipherManager, long maxEncryptions) {
        /* protected access because instantion only allowed via a DekManager */
        Objects.requireNonNull(edek);
        if (Objects.requireNonNull(key).isDestroyed()) {
            throw new IllegalArgumentException();
        }
        Objects.requireNonNull(cipherManager);
        if (maxEncryptions < 0) {
            throw new IllegalArgumentException();
        }
        this.edek = edek;
        this.atomicKey = new AtomicReference<>(key);
        this.cipherManager = cipherManager;
        this.remainingEncryptions = new AtomicLong(maxEncryptions);
        this.outstandingCryptors = new AtomicCryptorUsageCounter();
        if (maxEncryptions == 0) {
            outstandingCryptors.destroyForEncrypt();
        }
    }

    /**
     * Get an encryptor, good for at most {@code numEncryptions} {@linkplain Encryptor#encrypt(ByteBuffer, ByteBuffer, EncryptAllocator) encryptions}.
     * The caller must invoke {@link Encryptor#close()} after performing all the required operations.
     * Note that while this method is safe to call from multiple threads, the returned encryptor is not.
     * @param numEncryptions The number of encryption operations required
     * @return The encryptor.
     * @throws IllegalArgumentException If {@code numEncryptions} is less and or equal to zero.
     * @throws ExhaustedDekException If the DEK cannot support the given number of encryptions
     * @throws DestroyedDekException If the DEK has been {@linkplain #destroy()} destroyed.
     */
    public @NonNull Encryptor encryptor(int numEncryptions) {
        if (numEncryptions <= 0) {
            throw new IllegalArgumentException();
        }
        if (!outstandingCryptors.acquireEncryptorUsage()) {
            throw new DestroyedDekException();
        }
        if (remainingEncryptions.addAndGet(-numEncryptions) >= 0) {
            return new Encryptor(cipherManager, atomicKey.get(), numEncryptions);
        }
        else {
            outstandingCryptors.releaseEncryptorUsage();
            throw new ExhaustedDekException("This DEK does not have " + numEncryptions + " encryptions available");
        }
    }

    /**
     * Get a decryptor for this DEK.
     * Note that while this method is safe to call from multiple threads, the returned decryptor is not.
     * @return The decryptor.
     * @throws DestroyedDekException If the DEK has been {@linkplain #destroy()} destroyed.
     */
    public Decryptor decryptor() {
        if (!outstandingCryptors.acquireDecryptorUsage()) {
            throw new DestroyedDekException();
        }
        return new Decryptor(cipherManager, atomicKey.get());
    }

    /**
     * Destroy the key for encryption purposes.
     * This method is idempotent.
     * @see <a href="#destruction">Destruction</a> in the class Javadoc.
     */
    public void destroyForEncrypt() {
        maybeDestroyKey(outstandingCryptors.destroyForEncrypt());
    }

    /**
     * Destroy the key for both encryption and decryption purposes.
     * This is equivalent to calling both {@link #destroyForEncrypt()} and {@link #destroyForDecrypt()}.
     * This method is idempotent.
     * @see <a href="#destruction">Destruction</a> in the class Javadoc.
     */
    public void destroy() {
        // Using a dedicated operator reduces the contention on the atomic access
        maybeDestroyKey(outstandingCryptors.destroyForBoth());
    }

    /**
     * Destroy the key for decryption purposes.
     * This method is idempotent.
     * @see <a href="#destruction">Destruction</a> in the class Javadoc.
     */
    public void destroyForDecrypt() {
        maybeDestroyKey(outstandingCryptors.destroyForDecrypt());
    }

    private void maybeDestroyKey(boolean destroyed) {
        if (destroyed) {
            var key = atomicKey.getAndSet(null);
            if (key != null) {
                key.destroy();
            }
        }
    }

    public boolean isDestroyed() {
        SecretKey secretKey = atomicKey.get();
        return secretKey == null || secretKey.isDestroyed();
    }

    public E edek() {
        return edek;
    }

    /**
     * A means of performing a limited number of encryption operations without access to key material.
     */
    @NotThreadSafe
    public final class Encryptor implements AutoCloseable {
        private final Cipher cipher;
        private SecretKey key;
        private final Supplier<AlgorithmParameterSpec> paramSupplier;
        private final CipherManager cipherManager;
        private int numEncryptions;
        private boolean haveParameters = false;

        private Encryptor(CipherManager cipherManager, SecretKey key, int numEncryptions) {
            if (numEncryptions <= 0) {
                throw new IllegalArgumentException();
            }
            this.cipherManager = Objects.requireNonNull(cipherManager);
            this.key = Objects.requireNonNull(key);
            this.numEncryptions = numEncryptions;
            this.cipher = cipherManager.newCipher();
            this.paramSupplier = cipherManager.paramSupplier();
        }

        /**
         * @return The encrypted DEK
         */
        public @NonNull E edek() {
            return Dek.this.edek();
        }

        /**
         * Prepare to perform an encryption operation using the DEK.
         * This must be called before to a call to {@link #encrypt(ByteBuffer, ByteBuffer, EncryptAllocator)}.
         * @param paramAllocator A function that will return a buffer into which the cipher parameters will be written.
         * The function's argument is the number of bytes required for the cipher parameters.
         * @return The buffer returned from the {@code paramAllocator},
         * in which the cipher parameters have been written.
         * @throws DekUsageException If this Encryptor has run out of operations.
         * @throws BufferTooSmallException If the buffer returned by the {@code paramAllocator}
         * had too few bytes remaining for the parameters to be written completely.
         */
        public ByteBuffer generateParameters(@NonNull EncryptAllocator paramAllocator) {
            if (numEncryptions <= 0) {
                throw new DekUsageException("The Encryptor has no more operations allowed");
            }
            else {
                --numEncryptions;
                try {
                    AlgorithmParameterSpec params = paramSupplier.get();
                    cipher.init(Cipher.ENCRYPT_MODE, key, params);

                    int paramsSize = cipherManager.size(params);
                    var parametersBuffer = paramAllocator.buffer(paramsSize);
                    cipherManager.writeParameters(parametersBuffer, params);
                    parametersBuffer.flip();
                    haveParameters = true;
                    return parametersBuffer;
                }
                catch (BufferOverflowException e) {
                    throw new BufferTooSmallException();
                }
                catch (GeneralSecurityException e) {
                    throw new DekException(e);
                }
            }
        }

        /**
         * <p>Perform an encryption operation using the DEK.
         * This must be called after to a call to {@link #generateParameters(EncryptAllocator)}</p>
         *
         * <p>Like {@link Cipher#doFinal(ByteBuffer, ByteBuffer)}, the method is copy-safe:
         * The buffer returned by the {@code ciphertextAllocator}
         * may be the backed by the same buffer as the given {@code plaintext} and
         * no unprocessed plaintext data is overwritten when the result is copied into the ciphertext buffer</p>
         * @param plaintext The plaintext to be encrypted (between position and limit)
         * @param aad The AAD to be included in the encryption
         * The function's argument is the number of bytes required for the cipher parameters.
         * @param ciphertextAllocator A function that will return a buffer into which the ciphertext will be written.
         * The function's first argument is the number of bytes required for the cipher parameters.
         * The function's second argument is the number of bytes required for the ciphertext.
         * @return The buffer returned from the {@code ciphertextAllocator},
         * in which the ciphertext has been written.
         * @throws DekUsageException If this Encryptor has run out of operations.
         * @throws BufferTooSmallException If the buffer returned by the {@code ciphertextAllocator}
         * had too few bytes remaining for the ciphertext to be written completely.
         */
        public ByteBuffer encrypt(@NonNull ByteBuffer plaintext,
                                  @Nullable ByteBuffer aad,
                                  @NonNull EncryptAllocator ciphertextAllocator) {
            if (!haveParameters) {
                throw new IllegalStateException("Expecting a prior call to generateParameters()");
            }
            haveParameters = false;
            try {
                if (aad != null) {
                    cipher.updateAAD(aad);
                    aad.rewind();
                }
                int ciphertextSize = cipher.getOutputSize(plaintext.remaining());
                var ciphertext = ciphertextAllocator.buffer(ciphertextSize);
                var p = plaintext.position();
                cipher.doFinal(plaintext, ciphertext);
                plaintext.position(p);
                ciphertext.flip();

                if (numEncryptions == 0) {
                    close();
                }
                return ciphertext;
            }
            catch (ShortBufferException e) {
                throw new BufferTooSmallException();
            }
            catch (GeneralSecurityException e) {
                throw new DekException(e);
            }
        }

        @Override
        public void close() {
            if (key != null) {
                key = null;
                maybeDestroyKey(outstandingCryptors.releaseEncryptorUsage());
            }
        }

        public @NonNull CipherManager cipherManager() {
            return cipherManager;
        }

        public void recredit() {
            ++numEncryptions;
        }
    }

    /**
     * A means of performing decryption operations without access to key material.
     */
    @NotThreadSafe
    public final class Decryptor implements AutoCloseable {
        private final Cipher cipher;
        private SecretKey key;
        private final CipherManager cipherManager;

        private Decryptor(CipherManager cipherManager, SecretKey key) {
            this.cipher = cipherManager.newCipher();
            this.cipherManager = cipherManager;
            this.key = key;
        }

        /**
         * Perform an encryption operation using the DEK.
         * @param ciphertext The ciphertext.
         * @param aad The AAD.
         * @param parameterBuffer The buffer containing the cipher parameters.
         * @param plaintext The plaintext.
         */
        public void decrypt(@NonNull ByteBuffer ciphertext,
                            @Nullable ByteBuffer aad,
                            @NonNull ByteBuffer parameterBuffer,
                            @NonNull ByteBuffer plaintext) {
            try {
                var parameterSpec = cipherManager.readParameters(parameterBuffer);
                cipher.init(Cipher.DECRYPT_MODE, key, parameterSpec);
                if (aad != null) {
                    cipher.updateAAD(aad);
                }
                cipher.doFinal(ciphertext, plaintext);
            }
            catch (GeneralSecurityException e) {
                throw new DekException(e);
            }
        }

        @Override
        public void close() {
            if (key != null) {
                key = null;
                maybeDestroyKey(outstandingCryptors.releaseDecryptorUsage());
            }
        }
    }
}
