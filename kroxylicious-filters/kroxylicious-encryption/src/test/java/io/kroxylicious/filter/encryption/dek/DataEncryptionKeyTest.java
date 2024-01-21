/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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

    @Test
    void encryptNoAad() throws NoSuchAlgorithmException {
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

        ByteBuffer plaintext = ByteBuffer.wrap("hello, world".getBytes(StandardCharsets.UTF_8));
        assertThat(plaintext.position()).isZero();

        var buffer = ByteBuffer.allocate(100);
        ByteBuffer aad = null;
        encryptor.encrypt(plaintext,
                aad,
                size -> buffer.slice(0, size),
                (p, c) -> buffer.slice(p, c));

        // TODO assertions on the buffer

        assertThat(dek.isDestroyed())
                .describedAs("Key should be destroyed when no encryptions left")
                .isTrue();
        assertThat(plaintext.position())
                .describedAs("Position should be unchanged")
                .isZero();

        // Shouldn't be able to use the Encryptor again
        assertThatThrownBy(() -> encryptor.encrypt(plaintext,
                null,
                size -> ByteBuffer.allocate(size),
                (p, c) -> ByteBuffer.allocate(c)))
                .isExactlyInstanceOf(DekUsageException.class)
                .withFailMessage("The Encryptor has no more operations allowed");
        assertThat(plaintext.position())
                .describedAs("Position should be unchanged")
                .isZero();

        // It should be safe to close the encryptor
        encryptor.close();
    }

    // TODO encrypt with some AAD
    // TODO encrypt with too short cipher text buffer
    // TODO encrypt with too short parameters buffer
    // TODO decrypt
    // TODO decrypt with mismatching AAD
    // TODO decrypt with wrong IV
    // TODO round-trip

}
