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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import io.kroxylicious.filter.encryption.EncryptionException;
import io.kroxylicious.filter.encryption.inband.ExhaustedDekException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

class DekTest {

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void constructorThrowsOnDestroyedKey(CipherSpec cipherSpec) {
        var mock = mock(SecretKey.class);
        doReturn(true).when(mock).isDestroyed();
        assertThatThrownBy(() -> new Dek<>("edek", mock, cipherSpec, 100))
                .isExactlyInstanceOf(IllegalArgumentException.class);

    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void constructorThrowsOnNegativeExceptions(CipherSpec cipherSpec) {
        var mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        assertThatThrownBy(() -> new Dek<>("edek", mock, cipherSpec, -1))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void encryptorThrowsExhaustedDekExceptionOnDekWithZeroEncryptions(CipherSpec cipherSpec) {
        var mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        var dek = new Dek<>("edek", mock, cipherSpec, 0);
        assertThatThrownBy(() -> dek.encryptor(1))
                .isExactlyInstanceOf(ExhaustedDekException.class);
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void encryptorThrowsOnZeroEncryptions(CipherSpec cipherSpec) {
        var mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        var dek = new Dek<>("edek", mock, cipherSpec, 1);
        assertThatThrownBy(() -> dek.encryptor(0))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void encryptorThrowsOnNegativeEncryptions(CipherSpec cipherSpec) {
        var mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        var dek = new Dek<>("edek", mock, cipherSpec, 1);
        assertThatThrownBy(() -> dek.encryptor(-1))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void returnsEdek(CipherSpec cipherSpec) {
        var mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        String edek = "edek";
        Dek<String> dek = new Dek<>(edek, mock, cipherSpec, 100);
        assertThat(dek.encryptor(1).edek()).isSameAs(edek);

    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroyUnusedDek(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new Dek<>("edek", mock, cipherSpec, 100);

        // When
        dek.destroy();

        // Then
        Mockito.verify(mock).destroy();

        dek.destroy(); // This should be safe

        assertThatThrownBy(() -> dek.decryptor()).isExactlyInstanceOf(DestroyedDekException.class);
        assertThatThrownBy(() -> dek.encryptor(1)).isExactlyInstanceOf(DestroyedDekException.class);
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Encryptor_destroyThenClose(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new Dek<>("edek", mock, cipherSpec, 100);
        var cryptor = dek.encryptor(100);

        // When
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        dek.destroy(); // try destroying again

        cryptor.close();

        Mockito.verify(mock).destroy();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Encryptor_closeThenDestroy(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new Dek<>("edek", mock, cipherSpec, 100);
        var cryptor = dek.encryptor(100);

        cryptor.close();

        // When
        Mockito.verify(mock, never()).destroy();

        dek.destroy(); // try destroying again

        cryptor.close();

        Mockito.verify(mock).destroy();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy2Encryptor(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new Dek<>("edek", mock, cipherSpec, 100);
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

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy2EncryptorMultiClose(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new Dek<>("edek", mock, cipherSpec, 100);
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

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Decryptor_destroyThenClose(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new Dek<>("edek", mock, cipherSpec, 0);
        var cryptor = dek.decryptor();

        // When
        dek.destroy();

        // When
        Mockito.verify(mock, never()).destroy();

        dek.destroy(); // try destroying again

        cryptor.close();

        Mockito.verify(mock).destroy();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Decryptor_closeThenDestroy(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new Dek<>("edek", mock, cipherSpec, 0);
        var cryptor = dek.decryptor();

        // When
        cryptor.close();

        // When
        Mockito.verify(mock, never()).destroy();

        dek.destroy();

        Mockito.verify(mock).destroy();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy2Decryptor(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new Dek<>("edek", mock, cipherSpec, 0);
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

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy2DecryptorMultiClose(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new Dek<>("edek", mock, cipherSpec, 0);
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

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Encryptor1Decryptor_destroy(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new Dek<>("edek", mock, cipherSpec, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.decryptor();

        // When
        dek.destroy();
        dek.destroy(); // should be idempotent

        // When
        Mockito.verify(mock, never()).destroy(); // because cryptor1 and 2 outstanding

        cryptor1.close();

        Mockito.verify(mock, never()).destroy(); // because cryptor2 outstanding

        cryptor2.close();

        Mockito.verify(mock).destroy(); // should now be destroyed

        cryptor2.close(); // cryptor.close should be idempotent
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Encryptor1Decryptor_destroyForEncrypt(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new Dek<>("edek", mock, cipherSpec, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.decryptor();

        // When
        dek.destroyForEncrypt();
        dek.destroyForEncrypt(); // should be idempotent

        // When
        Mockito.verify(mock, never()).destroy(); // because cryptor1 and 2 outstanding

        cryptor1.close();

        Mockito.verify(mock, never()).destroy(); // because cryptor2 outstanding

        cryptor2.close();

        Mockito.verify(mock, never()).destroy(); // because only closed for encrypt

        dek.destroyForDecrypt();

        Mockito.verify(mock).destroy();

        cryptor2.close();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Encryptor1Decryptor_destroyForDecrypt(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        SecretKey mock = mock(SecretKey.class);
        doReturn(false).when(mock).isDestroyed();
        doNothing().when(mock).destroy();
        var dek = new Dek<>("edek", mock, cipherSpec, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.decryptor();

        // When
        dek.destroyForDecrypt();
        dek.destroyForDecrypt(); // should be idempotent

        // When
        Mockito.verify(mock, never()).destroy(); // because cryptor1 and 2 outstanding

        cryptor1.close();

        Mockito.verify(mock, never()).destroy(); // because cryptor2 outstanding

        cryptor2.close();

        Mockito.verify(mock, never()).destroy(); // because only closed for decrypt

        dek.destroyForEncrypt();

        Mockito.verify(mock).destroy();

        cryptor2.close();
    }

    // encrypt and decrypt with no AAD
    record EncryptInfo(SecretKey key,
                       ByteBuffer params,
                       ByteBuffer ciphertext) {}

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void encryptDecryptNoAad(CipherSpec cipherSpec) throws NoSuchAlgorithmException {
        ByteBuffer aad = null;
        var encryptInfo = encrypt(cipherSpec, aad, "hello, world");
        var roundTripped = decrypt(cipherSpec, aad, encryptInfo);
        assertThat(roundTripped).isEqualTo("hello, world");
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void encryptDecryptWithAad(CipherSpec cipherSpec) throws NoSuchAlgorithmException {
        ByteBuffer aad = ByteBuffer.wrap(new byte[]{ 42, 56, 89 });
        var encryptInfo = encrypt(cipherSpec, aad, "hello, world");
        var roundTripped = decrypt(cipherSpec, aad, encryptInfo);
        assertThat(roundTripped).isEqualTo("hello, world");
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void encryptDecryptWithMismatchingAad(CipherSpec cipherSpec) throws NoSuchAlgorithmException {
        ByteBuffer encryptAad = ByteBuffer.wrap(new byte[]{ 42, 56, 89 });
        var encryptInfo = encrypt(cipherSpec, encryptAad, "hello, world");

        ByteBuffer decryptAad = ByteBuffer.wrap(new byte[]{ 12, 12, 12 });
        assertThatThrownBy(() -> decrypt(cipherSpec, decryptAad, encryptInfo))
                .isExactlyInstanceOf(EncryptionException.class)
                .cause().isExactlyInstanceOf(AEADBadTagException.class);
    }

    private String decrypt(CipherSpec cipherSpec, ByteBuffer aad, EncryptInfo encryptInfo) {
        var params = encryptInfo.params();
        var ciphertext = encryptInfo.ciphertext();

        var dek = new Dek<>(encryptInfo.key().getEncoded(), encryptInfo.key(), cipherSpec, 1);
        var decryptor = dek.decryptor();

        var plaintext = ByteBuffer.allocate(1024);

        decryptor.decrypt(ciphertext, aad, params, plaintext);
        plaintext.flip();
        var bytes = new byte[plaintext.limit()];
        plaintext.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private EncryptInfo encrypt(CipherSpec cipherSpec, ByteBuffer aad, String plaintext) throws NoSuchAlgorithmException {
        // Given
        var generator = KeyGenerator.getInstance("AES");
        var key = generator.generateKey();
        var edek = key.getEncoded(); // For this test it doesn't matter than it's not, in fact, encrypted
        var dek = new Dek<>(edek, key, cipherSpec, 1);
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
                (p, c) -> slices[0] = buffer.slice(0, p),
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
                (p, c) -> ByteBuffer.allocate(p),
                (p, c) -> ByteBuffer.allocate(c)))
                .isExactlyInstanceOf(DekUsageException.class)
                .hasMessage("The Encryptor has no more operations allowed");
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
