/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.filter.encryption.EncryptionException;
import io.kroxylicious.filter.encryption.inband.ExhaustedDekException;

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
 * {@link Encryptor#encrypt(ByteBuffer, ByteBuffer, EncryptAllocator, EncryptAllocator) Encryptor.encrypt()}
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

    private static final Logger LOGGER = LoggerFactory.getLogger(Dek.class);

    private static final Map<Class<? extends Destroyable>, Boolean> LOGGED_DESTROY_FAILED = new ConcurrentHashMap<>();

    private final E edek;
    // Note: checks for outstandingCryptors==END <em>happens-before</em> changes to atomicKey.
    // It's this that provides a guarantee that a cryptor should never see a
    // destroyed or null key, but that keys should be destroyed and nullieifed as soon
    // as possible once all outstanding cryptors are closed.
    private final AtomicReference<SecretKey> atomicKey;

    private final AtomicLong remainingEncryptions;

    /**
     * <p>We use an AtomicLong, but it's really an atomic pair of ints acting as
     * reference counts for the number of outstanding encryptors and decryptors.
     * The counts start from 1 (i.e. 1 means no outstanding cryptor), increments with
     * each cryptor created and decrements with each cryptor closed.
     * Calling destroyFor(En|De)crypt flips the sign of the count
     * and inverts the direction of the counting (i.e. cryptor close now increments).
     * This means that we hit -1 when all outstanding cryptors have been closed.</p>
     *
     * <p>Here's a worked example:</p>
     * <table>
     *     <tr><th>Action</th>                        <th>Encryptor Count</th> <th>Decryptor Count</th></tr>
     *     <tr><td>«{@link #START}»</td>              <td>1</td>               <td>1</td>    </tr>
     *     <tr><td>encryptor()</td>                   <td>2</td>               <td>1</td>    </tr>
     *     <tr><td>encryptor()</td>                   <td>3</td>               <td>1</td>    </tr>
     *     <tr><td>decryptor()</td>                   <td>3</td>               <td>2</td>    </tr>
     *     <tr><td>Encryptor.close()</td>             <td>2</td>               <td>2</td>    </tr>
     *     <tr><td>{@link #destroyForEncrypt()}</td>  <td>-2</td>               <td>2</td>    </tr>
     *     <tr><td>Encryptor.close()</td>             <td>-1</td>               <td>2</td>    </tr>
     *     <tr><td>{@link #destroyForDecrypt()}       </td>  <td>-1</td>               <td>-2</td>   </tr>
     *     <tr><td>Decryptor.close()</td>             <td>-1</td>               <td>-1</td>    </tr>
     *     <tr><td colspan="3">«{@link #END}» // key gets destroyed </td>    </tr>
     * </table>
     */
    final AtomicLong outstandingCryptors;

    private static final long START = combine(1, 1);
    private static final long END = combine(-1, -1);
    private final CipherSpec cipherSpec;

    /** Combine two int reference counts into a single long */
    static long combine(int encryptors, int decryptors) {
        return ((long) encryptors) << Integer.SIZE | 0xFFFFFFFFL & decryptors;
    }

    /** Extract the encryptor reference count from a long */
    static int encryptors(long combined) {
        return (int) (combined >> Integer.SIZE);
    }

    /** Extract the decryptor reference count from a long */
    static int decryptors(long combined) {
        return (int) combined;
    }

    /** Unary operator for acquiring an encryptor */
    static long acquireEncryptor(long combined) {
        final int encryptors = encryptors(combined);
        final int decryptors = decryptors(combined);
        if (encryptors > 0) { // not destroyed for encrypt
            return combine(encryptors + 1, decryptors);
        }
        else {
            return combined;
        }
    }

    /** Unary operator for acquiring a decryptor */
    static long acquireDecryptor(long combined) {
        final int encryptors = encryptors(combined);
        final int decryptors = decryptors(combined);
        if (decryptors > 0) { // not destroyed for decrypt
            return combine(encryptors, decryptors + 1);
        }
        else {
            return combined;
        }
    }

    /** Unary operator for releasing an encryptor */
    static long releaseEncryptor(long combined) {
        final int encryptors = encryptors(combined);
        final int decryptors = decryptors(combined);
        if (encryptors > 0) {
            return combine(encryptors - 1, decryptors);
        }
        else {
            return combine(encryptors + 1, decryptors);
        }
    }

    /** Unary operator for releasing a decryptor */
    static long releaseDecryptor(long combined) {
        final int encryptors = encryptors(combined);
        final int decryptors = decryptors(combined);
        if (decryptors > 0) {
            return combine(encryptors, decryptors - 1);
        }
        else {
            return combine(encryptors, decryptors + 1);
        }
    }

    /** Unary operator for "destroying" the key for encryption */
    static long commenceDestroyEncryptor(long combined) {
        final int encryptors = encryptors(combined);
        final int decryptors = decryptors(combined);
        if (encryptors < 0) { // no-op if already destroyed for encrypt
            return combined;
        }
        return combine(-encryptors, decryptors);
    }

    /** Unary operator for "destroying" the key for decryption */
    static long commenceDestroyDecryptor(long combined) {
        final int encryptors = encryptors(combined);
        final int decryptors = decryptors(combined);
        if (decryptors < 0) { // no-op if already destroyed for decrypt
            return combined;
        }
        return combine(encryptors, -decryptors);
    }

    /** Unary operator for "destroying" the key for both encryption and decryption */
    static long commenceDestroyBoth(long combined) {
        final int encryptors = encryptors(combined);
        final int decryptors = decryptors(combined);
        final int newEncryptors;
        final int newDecryptors;
        if (encryptors < 0) {
            if (decryptors < 0) {
                return combined; // no-op if already destroyed for both
            }
            else {
                newDecryptors = -decryptors;
            }
            newEncryptors = encryptors;
        }
        else {
            if (decryptors < 0) {
                newDecryptors = decryptors;
            }
            else {
                newDecryptors = -decryptors;
            }
            newEncryptors = -encryptors;
        }
        return combine(newEncryptors, newDecryptors);
    }

    Dek(@NonNull E edek, @NonNull SecretKey key, @NonNull CipherSpec cipherSpec, long maxEncryptions) {
        /* protected access because instantion only allowed via a DekManager */
        Objects.requireNonNull(edek);
        if (Objects.requireNonNull(key).isDestroyed()) {
            throw new IllegalArgumentException();
        }
        Objects.requireNonNull(cipherSpec);
        if (maxEncryptions < 0) {
            throw new IllegalArgumentException();
        }
        this.edek = edek;
        this.atomicKey = new AtomicReference<>(key);
        this.cipherSpec = cipherSpec;
        this.remainingEncryptions = new AtomicLong(maxEncryptions);
        this.outstandingCryptors = new AtomicLong(START);
    }

    /**
     * Get an encryptor, good for at most {@code numEncryptions} {@linkplain Encryptor#encrypt(ByteBuffer, ByteBuffer, EncryptAllocator, EncryptAllocator) encryptions}.
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
        if (remainingEncryptions.addAndGet(-numEncryptions) >= 0) {
            if (encryptors(outstandingCryptors.updateAndGet(Dek::acquireEncryptor)) <= 0) {
                throw new DestroyedDekException();
            }
            return new Encryptor(cipherSpec, atomicKey.get(), numEncryptions);
        }
        throw new ExhaustedDekException("This DEK does not have " + numEncryptions + " encryptions available");
    }

    /**
     * Get a decryptor for this DEK.
     * Note that while this method is safe to call from multiple threads, the returned decryptor is not.
     * @return The decryptor.
     * @throws DestroyedDekException If the DEK has been {@linkplain #destroy()} destroyed.
     */
    public Decryptor decryptor() {
        if (decryptors(outstandingCryptors.updateAndGet(Dek::acquireDecryptor)) <= 0) {
            throw new DestroyedDekException();
        }
        return new Decryptor(cipherSpec, atomicKey.get());
    }

    /**
     * Destroy the key for encryption purposes.
     * This method is idempotent.
     * @see <a href="#destruction">Destruction</a> in the class Javadoc.
     */
    public void destroyForEncrypt() {
        maybeDestroyKey(Dek::commenceDestroyEncryptor);
    }

    /**
     * Destroy the key for both encryption and decryption purposes.
     * This is equivalent to calling both {@link #destroyForEncrypt()} and {@link #destroyForDecrypt()}.
     * This method is idempotent.
     * @see <a href="#destruction">Destruction</a> in the class Javadoc.
     */
    public void destroy() {
        // Using a dedicated operator reduces the contention on the atomic access
        maybeDestroyKey(Dek::commenceDestroyBoth);
    }

    /**
     * Destroy the key for decryption purposes.
     * This method is idempotent.
     * @see <a href="#destruction">Destruction</a> in the class Javadoc.
     */
    public void destroyForDecrypt() {
        maybeDestroyKey(Dek::commenceDestroyDecryptor);
    }

    private void maybeDestroyKey(LongUnaryOperator updateFunction) {
        if (outstandingCryptors.updateAndGet(updateFunction) == END) {
            var key = atomicKey.getAndSet(null);
            if (key != null) {
                try {
                    key.destroy();
                }
                catch (DestroyFailedException e) {
                    var cls = key.getClass();
                    if (LOGGER.isWarnEnabled()) {
                        LOGGED_DESTROY_FAILED.computeIfAbsent(cls, alsoCls -> {
                            LOGGER.warn("Failed to destroy an instance of {}. "
                                    + "Note: this message is logged once per class even though there may be many occurrences of this event. "
                                    + "This event can happen because the JRE's SecretKeySpec class does not override the destroy() method.",
                                    alsoCls, e);
                            return Boolean.TRUE;
                        });
                    }
                }
            }
        }
    }

    public boolean isDestroyed() {
        SecretKey secretKey = atomicKey.get();
        return secretKey == null || secretKey.isDestroyed();
    }

    /**
     * A means of performing a limited number of encryption operations without access to key material.
     */
    @NotThreadSafe
    public final class Encryptor implements AutoCloseable {
        private final Cipher cipher;
        private SecretKey key;
        private final Supplier<AlgorithmParameterSpec> paramSupplier;
        private final CipherSpec cipherSpec;
        private int numEncryptions;

        private Encryptor(CipherSpec cipherSpec, SecretKey key, int numEncryptions) {
            if (numEncryptions <= 0) {
                throw new IllegalArgumentException();
            }
            this.cipher = cipherSpec.newCipher();
            this.paramSupplier = cipherSpec.paramSupplier();
            this.cipherSpec = cipherSpec;
            this.key = key;
            this.numEncryptions = numEncryptions;
        }

        /**
         * @return The encrypted key
         */
        public E edek() {
            return edek;
        }

        /**
         * Perform an encryption operation using the DEK.
         * @param plaintext The plaintext to be encrypted (between position and limit)
         * @param aad The AAD to be included in the encryption
         * @param paramAllocator A function that will return a buffer into which the cipher parameters will be written.
         * The function's argument is the number of bytes required for the cipher parameters.
         * @param ciphertextAllocator A function that will return a buffer into which the ciphertext will be written.
         * The function's first argument is the number of bytes required for the cipher parameters.
         * The function's second argument is the number of bytes required for the ciphertext.
         * @throws DekUsageException If this Encryptor has run out of operations.
         */
        public void encrypt(@NonNull ByteBuffer plaintext,
                            @Nullable ByteBuffer aad,
                            @NonNull EncryptAllocator paramAllocator,
                            @NonNull EncryptAllocator ciphertextAllocator) {

            if (--numEncryptions >= 0) {
                try {
                    AlgorithmParameterSpec params = paramSupplier.get();
                    cipher.init(Cipher.ENCRYPT_MODE, key, params);

                    int paramsSize = cipherSpec.size(params);
                    int ciphertextSize = cipher.getOutputSize(plaintext.remaining());
                    var parametersBuffer = paramAllocator.buffer(paramsSize, ciphertextSize);
                    var ciphertext = ciphertextAllocator.buffer(paramsSize, ciphertextSize);
                    cipherSpec.writeParameters(parametersBuffer, params);
                    parametersBuffer.flip();
                    if (aad != null) {
                        cipher.updateAAD(aad);
                        aad.rewind();
                    }
                    var p = plaintext.position();
                    cipher.doFinal(plaintext, ciphertext);
                    plaintext.position(p);
                    ciphertext.flip();

                    if (numEncryptions == 0) {
                        close();
                    }
                    return;
                }
                catch (GeneralSecurityException e) {
                    throw new EncryptionException(e);
                }
            }
            throw new DekUsageException("The Encryptor has no more operations allowed");
        }

        @Override
        public void close() {
            if (key != null) {
                key = null;
                maybeDestroyKey(Dek::releaseEncryptor);
            }
        }
    }

    /**
     * A means of performing decryption operations without access to key material.
     */
    @NotThreadSafe
    public final class Decryptor implements AutoCloseable {
        private final Cipher cipher;
        private SecretKey key;
        private final CipherSpec cipherSpec;

        private Decryptor(CipherSpec cipherSpec, SecretKey key) {
            this.cipher = cipherSpec.newCipher();
            this.cipherSpec = cipherSpec;
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
                var parameterSpec = cipherSpec.readParameters(parameterBuffer);
                cipher.init(Cipher.DECRYPT_MODE, key, parameterSpec);
                if (aad != null) {
                    cipher.updateAAD(aad);
                }
                cipher.doFinal(ciphertext, plaintext);
            }
            catch (GeneralSecurityException e) {
                throw new EncryptionException(e);
            }
        }

        @Override
        public void close() {
            if (key != null) {
                key = null;
                maybeDestroyKey(Dek::releaseDecryptor);
            }
        }
    }
}
