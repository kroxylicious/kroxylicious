/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import io.kroxylicious.filter.encryption.inband.ExhaustedDekException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.GCMParameterSpec;
import javax.security.auth.DestroyFailedException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class DataEncryptionKey<E> {
    private final E edek;
    private final SecretKey key;
    private final AtomicInteger remainingEncryptions;
    // Initially 0, incremented when an Encryptor or Decryptor is created
    // Negated when destroyed

    // 1 start
    // +1=2 encryptor
    // +1=3 encryptor
    // +1=4 encryptor
    // -1=3 encryptor.finish // return
    // -3=0 destroy()// updateAndGet(if current value < 0 don't change, otherwise negate)
    // -1=-4 encryptor.finish
    // -1=-5 encryptor.finish => key.destroy
    private final AtomicReference<CountDownLatch> sem;

    DataEncryptionKey(E edek, SecretKey key, int maxEncryptions) {
        this.edek = edek;
        this.key = key;
        this.remainingEncryptions = new AtomicInteger(maxEncryptions);
        this.outstandingCryptors = new AtomicInteger();
    }

    /**
     * Get an encryptor, good for at most {@code numEncryptions}.
     * @param numEncryptions The number of encryption operations required
     * @return The encryptor.
     */
    public Encryptor encryptor(int numEncryptions) {
        if (numEncryptions < 0) {
            throw new IllegalArgumentException();
        }
        if (remainingEncryptions.addAndGet(-numEncryptions) >= 0) {
            var cipher = Cipher.getInstance("AES");
            Encryptor encryptor = new Encryptor(cipher, key, numEncryptions);
            outstandingCryptors.updateAndGet(value -> value + 1);
            return encryptor;
        }
        throw new ExhaustedDekException();
    }

    public Decryptor decryptor() {
        if (remainingEncryptions.addAndGet(-numEncryptions) >= 0) {
            var cipher = Cipher.getInstance("AES");
            Decryptor decryptor = new Decryptor(cipher, key);
            outstandingCryptors.incrementAndGet();
            return decryptor;
        }
        throw new ExhaustedDekException();
    }

    public void destroy() throws InterruptedException, DestroyFailedException {
        sem.updateAndGet(x -> x == null ? new CountDownLatch(1) : x).await();
        key.destroy();
    }

    private void returnLease() {
        if (outstandingCryptors.decrementAndGet(value -> value > 0 ? -value : value) == ???) {
            key.destroy();
        }
    }


    public final class Encryptor {
        private final Cipher cipher;
        private final SecretKey key;
        private int numEncryptions;

        private Encryptor(Cipher cipher, SecretKey key, int numEncryptions) {
            if (numEncryptions <= 0) {
                throw new IllegalArgumentException();
            }
            this.cipher = cipher;
            this.key = key;
            this.numEncryptions = numEncryptions;
        }

        public E edek() {
            return edek;
        }

        public void encrypt(ByteBuffer plaintext, ByteBuffer aad, ByteBuffer ciphertext) throws InvalidKeyException, ShortBufferException, IllegalBlockSizeException, BadPaddingException {
            if (--numEncryptions >= 0) {
                cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(1, 2));
                cipher.updateAAD(aad);
                cipher.doFinal(plaintext, ciphertext);
                return;
            }
            throw new RuntimeException();
        }

        public void finish() {
            returnLease();
        }
    }

    public final class Decryptor {
        private final Cipher cipher;
        private final SecretKey key;
        private int numEncryptions;

        private Decryptor(Cipher cipher, SecretKey key, int numEncryptions) {
            this.cipher = cipher;
            this.key = key;
            this.numEncryptions = numEncryptions;
        }

        public void decrypt(ByteBuffer ciphertext, ByteBuffer aad, ByteBuffer plaintext) throws InvalidKeyException, ShortBufferException, IllegalBlockSizeException, BadPaddingException {
            if (--numEncryptions >= 0) {
                cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(1, 2));
                cipher.updateAAD(aad);
                cipher.doFinal(ciphertext, plaintext);
                return;
            }
            throw new RuntimeException();
        }

        public void finish() {
            returnLease();
        }
    }
}
