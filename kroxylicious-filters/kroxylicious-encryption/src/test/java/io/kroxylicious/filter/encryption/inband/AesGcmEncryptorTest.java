/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.AEADBadTagException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;
import javax.security.auth.DestroyFailedException;

import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.EncryptionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AesGcmEncryptorTest {

    @Test
    void shouldRejectInvalidUsagePattern() throws NoSuchAlgorithmException {
        var keygen = KeyGenerator.getInstance("AES");
        SecretKey key = keygen.generateKey();
        var enc = AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), key);
        var plaintext = "hello, world!";
        byte[] plaintextBytes = plaintext.getBytes(StandardCharsets.UTF_8);

        // A call to encrypt without prior call to outputSize
        assertThrows(IllegalStateException.class, () -> enc.encrypt(null, null));

        var size = enc.outputSize(plaintextBytes.length);
        var ciphertext = ByteBuffer.allocate(size);
        enc.encrypt(ByteBuffer.wrap(plaintextBytes), ciphertext);
        // Two calls to encrypt in a row
        assertThrows(IllegalStateException.class, () -> enc.encrypt(null, null));

        assertThrows(IllegalStateException.class, () -> enc.decrypt(null, null));
    }

    @Test
    void shouldDestroy() throws NoSuchAlgorithmException, DestroyFailedException {
        var keygen = KeyGenerator.getInstance("AES");
        SecretKey key = keygen.generateKey();
        var enc = AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), key);
        assertThrows(DestroyFailedException.class, enc::destroy);
        // ^^ this is not the behaviour we want, but it's the behaviour we get (from the in memory KMS at least)
        assertTrue(enc.isDestroyed());

        enc.destroy();
        // once destroyed we expect a 2nd call to not throw
    }

    @Test
    void shouldRoundTrip() throws NoSuchAlgorithmException {
        var keygen = KeyGenerator.getInstance("AES");
        SecretKey key = keygen.generateKey();
        var enc = AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), key);
        var plaintext = "hello, world!";
        byte[] plaintextBytes = plaintext.getBytes(StandardCharsets.UTF_8);
        var size = enc.outputSize(plaintextBytes.length);
        var ciphertext = ByteBuffer.allocate(size);
        enc.encrypt(ByteBuffer.wrap(plaintextBytes), ciphertext);
        assertFalse(ciphertext.hasRemaining());
        ciphertext.flip();

        var roundTripped = ByteBuffer.allocate(plaintextBytes.length);
        var dec = AesGcmEncryptor.forDecrypt(key);
        dec.decrypt(ciphertext, roundTripped);
        assertEquals(plaintext, new String(roundTripped.array(), StandardCharsets.UTF_8));
    }

    @Test
    void shouldThrowOnShortBuffer() throws NoSuchAlgorithmException {
        var keygen = KeyGenerator.getInstance("AES");
        var enc = AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), keygen.generateKey());
        var plaintext = "hello, world!";
        byte[] plaintextBytes = plaintext.getBytes(StandardCharsets.UTF_8);
        var size = enc.outputSize(plaintextBytes.length);
        var ciphertext = ByteBuffer.allocate(size - 1);
        ByteBuffer wrap = ByteBuffer.wrap(plaintextBytes);
        var e = assertThrows(EncryptionException.class, () -> enc.encrypt(wrap, ciphertext));
        assertInstanceOf(ShortBufferException.class, e.getCause());
    }

    @Test
    void shouldThrowOnBadKey() {
        var badKey = new SecretKeySpec(new byte[3], "AES");
        var enc = AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), badKey);
        var plaintext = "hello, world!";
        byte[] plaintextBytes = plaintext.getBytes(StandardCharsets.UTF_8);
        var e = assertThrows(EncryptionException.class, () -> enc.outputSize(plaintextBytes.length));
        assertInstanceOf(InvalidKeyException.class, e.getCause());
    }

    @Test
    void shouldThrowDeserializingUnexpectedIvLength() throws NoSuchAlgorithmException {
        var keygen = KeyGenerator.getInstance("AES");
        var enc = AesGcmEncryptor.forDecrypt(keygen.generateKey());

        ByteBuffer encoded = ByteBuffer.wrap(new byte[]{ 0, (byte) 1 });
        ByteBuffer output = ByteBuffer.allocate(10);
        var e = assertThrows(EncryptionException.class, () -> enc.decrypt(encoded, output));
        assertEquals("Unexpected IV length", e.getMessage());
    }

    @Test
    void shouldDetectChangedCiphertext() throws NoSuchAlgorithmException {
        var keygen = KeyGenerator.getInstance("AES");
        var enc = AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), keygen.generateKey());
        var plaintext = "hello, world!";
        byte[] plaintextBytes = plaintext.getBytes(StandardCharsets.UTF_8);
        var size = enc.outputSize(plaintextBytes.length);
        var ciphertext = ByteBuffer.allocate(size);
        enc.encrypt(ByteBuffer.wrap(plaintextBytes), ciphertext);
        assertFalse(ciphertext.hasRemaining());
        int pos = ciphertext.position() - 1;
        ciphertext.put(pos, (byte) (ciphertext.get(pos) + 1));
        ciphertext.flip();
        var roundTripped = ByteBuffer.allocate(plaintextBytes.length);

        var dec = AesGcmEncryptor.forDecrypt(keygen.generateKey());
        var e = assertThrows(EncryptionException.class, () -> dec.decrypt(ciphertext, roundTripped));
        assertInstanceOf(AEADBadTagException.class, e.getCause());

    }

    @Test
    void shouldThrowOnUnknownVersion() throws NoSuchAlgorithmException {
        var keygen = KeyGenerator.getInstance("AES");
        var enc = AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), keygen.generateKey());
        var plaintext = "hello, world!";
        byte[] plaintextBytes = plaintext.getBytes(StandardCharsets.UTF_8);
        var size = enc.outputSize(plaintextBytes.length);
        var ciphertext = ByteBuffer.allocate(size);
        enc.encrypt(ByteBuffer.wrap(plaintextBytes), ciphertext);
        assertFalse(ciphertext.hasRemaining());
        ciphertext.put(0, Byte.MAX_VALUE);
        ciphertext.flip();
        var roundTripped = ByteBuffer.allocate(plaintextBytes.length);

        var dec = AesGcmEncryptor.forDecrypt(keygen.generateKey());
        var e = assertThrows(EncryptionException.class, () -> dec.decrypt(ciphertext, roundTripped));
        assertEquals("Unknown version 127", e.getMessage());
    }

    @Test
    void shouldByCopySafe() throws NoSuchAlgorithmException {
        var keygen = KeyGenerator.getInstance("AES");
        SecretKey key = keygen.generateKey();
        var enc = AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), key);

        var str = "hello, world!";
        var buffer = ByteBuffer.allocate(200);
        var plaintext = buffer.duplicate();
        plaintext.put(str.getBytes(StandardCharsets.UTF_8));
        plaintext.flip();

        var ciphertext = buffer.duplicate();
        enc.outputSize(plaintext.limit());
        enc.encrypt(plaintext, ciphertext);

        ciphertext.flip();

        var roundTripped = buffer.duplicate();
        var dec = AesGcmEncryptor.forDecrypt(key);
        dec.decrypt(ciphertext, roundTripped);
        assertEquals(str, StandardCharsets.UTF_8.decode(roundTripped.flip()).toString());
    }

}
