/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;

import javax.crypto.AEADBadTagException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.filter.encryption.EncryptionException;
import io.kroxylicious.filter.encryption.inband.ExhaustedDekException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

class DataEncryptionKeyTest {

    @Test
    void constructorThrowsOnDestroyedKey() {
        var mock = mock(SecretKey.class);
        doReturn(true).when(mock).isDestroyed();
        assertThatThrownBy(() -> new DataEncryptionKey<>("edek", mock, 100))
                .isExactlyInstanceOf(IllegalArgumentException.class);

    }

    @Test
    void constructorThrowsOnNegativeExceptions() {
        var mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        assertThatThrownBy(() -> new DataEncryptionKey<>("edek", mock, -1))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void encryptorThrowsExhaustedDekExceptionOnDekWithZeroEncryptions() {
        var mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        var dek = new DataEncryptionKey<>("edek", mock, 0);
        assertThatThrownBy(() -> dek.encryptor(1))
                .isExactlyInstanceOf(ExhaustedDekException.class);
    }

    @Test
    void encryptorThrowsOnZeroEncryptions() {
        var mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        var dek = new DataEncryptionKey<>("edek", mock, 1);
        assertThatThrownBy(() -> dek.encryptor(0))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void encryptorThrowsOnNegativeEncryptions() {
        var mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        var dek = new DataEncryptionKey<>("edek", mock, 1);
        assertThatThrownBy(() -> dek.encryptor(-1))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void returnsEdek() {
        var mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        String edek = "edek";
        DataEncryptionKey<String> dek = new DataEncryptionKey<>(edek, mock, 100);
        assertThat(dek.encryptor(1).edek()).isSameAs(edek);

    }

    @Test
    void destroyUnusedDek() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 100);

        // When
        dek.destroy();

        // Then
        Mockito.verify(mock).destroy();

        dek.destroy(); // This should be safe

        assertThatThrownBy(() -> dek.decryptor()).isExactlyInstanceOf(DestroyedDekException.class);
        assertThatThrownBy(() -> dek.encryptor(1)).isExactlyInstanceOf(DestroyedDekException.class);
    }

    @Test
    void destroy1Encryptor_destroyThenClose() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 100);
        var cryptor = dek.encryptor(100);

        // When
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        dek.destroy(); // try destroying again

        cryptor.close();

        Mockito.verify(mock).destroy();
    }

    @Test
    void destroy1Encryptor_closeThenDestroy() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 100);
        var cryptor = dek.encryptor(100);

        cryptor.close();

        // When
        Mockito.verify(mock, never()).destroy();

        dek.destroy(); // try destroying again

        cryptor.close();

        Mockito.verify(mock).destroy();
    }

    @Test
    void destroy2Encryptor() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.encryptor(50);

        // When
        dek.destroy();
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        cryptor1.close();

        Mockito.verify(mock, never()).destroy();

        cryptor2.close();

        Mockito.verify(mock).destroy();
    }

    @Test
    void destroy2EncryptorMultiClose() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.encryptor(50);

        // When
        dek.destroy();
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        cryptor1.close();
        cryptor1.close();

        Mockito.verify(mock, never()).destroy();

        cryptor2.close();

        Mockito.verify(mock).destroy();

        cryptor1.close();
    }

    @Test
    void destroy1Decryptor_destroyThenClose() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 0);
        var cryptor = dek.decryptor();

        // When
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        dek.destroy(); // try destroying again

        cryptor.close();

        Mockito.verify(mock).destroy();
    }

    @Test
    void destroy1Decryptor_closeThenDestroy() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 0);
        var cryptor = dek.decryptor();

        // When
        cryptor.close();

        // When
        Mockito.verify(mock, never()).destroy();

        dek.destroy();

        Mockito.verify(mock).destroy();
    }

    @Test
    void destroy2Decryptor() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 0);
        var cryptor1 = dek.decryptor();
        var cryptor2 = dek.decryptor();

        // When
        dek.destroy();
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        cryptor1.close();

        Mockito.verify(mock, never()).destroy();

        cryptor2.close();

        Mockito.verify(mock).destroy();
    }

    @Test
    void destroy2DecryptorMultiClose() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 0);
        var cryptor1 = dek.decryptor();
        var cryptor2 = dek.decryptor();

        // When
        dek.destroy();
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        cryptor1.close();
        cryptor1.close();

        Mockito.verify(mock, never()).destroy();

        cryptor2.close();

        Mockito.verify(mock).destroy();

        cryptor1.close();
    }

    @Test
    void destroy1Encryptor1Decryptor() throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new DataEncryptionKey<>("edek", mock, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.decryptor();

        // When
        dek.destroy();
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        cryptor1.close();

        Mockito.verify(mock, never()).destroy();

        cryptor2.close();

        Mockito.verify(mock).destroy();

        cryptor2.close();
    }

    // encrypt and decrypt with no AAD
    record EncryptInfo(SecretKey key,
                       ByteBuffer params,
                       ByteBuffer ciphertext) {}

    @Test
    void encryptDecryptNoAad() throws NoSuchAlgorithmException {
        ByteBuffer aad = null;
        var encryptInfo = encrypt(aad, "hello, world");
        var roundTripped = decrypt(aad, encryptInfo);
        assertThat(roundTripped).isEqualTo("hello, world");
    }

    @Test
    void encryptDecryptWithAad() throws NoSuchAlgorithmException {
        ByteBuffer aad = ByteBuffer.wrap(new byte[]{ 42, 56, 89 });
        var encryptInfo = encrypt(aad, "hello, world");
        var roundTripped = decrypt(aad, encryptInfo);
        assertThat(roundTripped).isEqualTo("hello, world");
    }

    @Test
    void encryptDecryptWithMismatchingAad() throws NoSuchAlgorithmException {
        ByteBuffer encryptAad = ByteBuffer.wrap(new byte[]{ 42, 56, 89 });
        var encryptInfo = encrypt(encryptAad, "hello, world");

        ByteBuffer decryptAad = ByteBuffer.wrap(new byte[]{ 12, 12, 12 });
        assertThatThrownBy(() -> decrypt(decryptAad, encryptInfo))
                .isExactlyInstanceOf(EncryptionException.class)
                .cause().isExactlyInstanceOf(AEADBadTagException.class);
    }

    private String decrypt(ByteBuffer aad, EncryptInfo encryptInfo) {
        var params = encryptInfo.params();
        var ciphertext = encryptInfo.ciphertext();

        var dek = new DataEncryptionKey<>(encryptInfo.key().getEncoded(), encryptInfo.key(), 1);
        var decryptor = dek.decryptor();

        var plaintext = ByteBuffer.allocate(1024);

        decryptor.decrypt(ciphertext, aad, params, plaintext);
        plaintext.flip();
        var bytes = new byte[plaintext.limit()];
        plaintext.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private EncryptInfo encrypt(ByteBuffer aad, String plaintext) throws NoSuchAlgorithmException {
        // Given
        var generator = KeyGenerator.getInstance("AES");
        var key = generator.generateKey();
        var edek = key.getEncoded(); // For this test it doesn't matter than it's not, in fact, encrypted
        var dek = new DataEncryptionKey<>(edek, key, 1);
        var encryptor = dek.encryptor(1);

        // Destroy the DEK: We expect the encryptor should still be usable
        dek.destroy();
        assertThat(dek.isDestroyed())
                .describedAs("Key should be not be destroyed because some encryptions are left")
                .isFalse();

        assertThatThrownBy(() -> dek.encryptor(1))
                .describedAs("Expect to not be able to get another encoder")
                .isExactlyInstanceOf(ExhaustedDekException.class);

        ByteBuffer plaintextBuffer = ByteBuffer.wrap(plaintext.getBytes(StandardCharsets.UTF_8));
        assertThat(plaintextBuffer.position()).isZero();

        var buffer = ByteBuffer.allocate(100);
        var slices = new ByteBuffer[2];

        encryptor.encrypt(plaintextBuffer,
                aad,
                size -> slices[0] = buffer.slice(0, size),
                (p, c) -> slices[1] = buffer.slice(p, c));

        // TODO assertions on the buffer

        assertThat(dek.isDestroyed())
                .describedAs("Key should be destroyed when no encryptions left")
                .isTrue();
        assertThat(plaintextBuffer.position())
                .describedAs("Position should be unchanged")
                .isZero();

        // Shouldn't be able to use the Encryptor again
        assertThatThrownBy(() -> encryptor.encrypt(plaintextBuffer,
                null,
                size -> ByteBuffer.allocate(size),
                (p, c) -> ByteBuffer.allocate(c)))
                .isExactlyInstanceOf(DekUsageException.class)
                .withFailMessage("The Encryptor has no more operations allowed");
        assertThat(plaintextBuffer.position())
                .describedAs("Position should be unchanged")
                .isZero();

        // It should be safe to close the encryptor
        encryptor.close();

        return new EncryptInfo(key, slices[0], slices[1]);
    }

    // TODO encrypt with too short cipher text buffer
    // TODO encrypt with too short parameters buffer
    // TODO decrypt with wrong IV

}
