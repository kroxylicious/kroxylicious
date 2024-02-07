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

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DekTest {

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void constructorThrowsOnDestroyedKey(CipherSpec cipherSpec) {
        var key = makeKey();
        key.destroy();
        assertThatThrownBy(() -> new Dek<>("edek", key, cipherSpec, 100))
                .isExactlyInstanceOf(IllegalArgumentException.class);

    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void constructorThrowsOnNegativeExceptions(CipherSpec cipherSpec) {
        var key = makeKey();
        assertThatThrownBy(() -> new Dek<>("edek", key, cipherSpec, -1))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void encryptorThrowsExhaustedDekExceptionOnDekWithZeroEncryptions(CipherSpec cipherSpec) {
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 0);
        assertThatThrownBy(() -> dek.encryptor(1))
                .isExactlyInstanceOf(ExhaustedDekException.class);
    }

    @NonNull
    private DestroyableRawSecretKey makeKey() {
        return new DestroyableRawSecretKey("AES", new byte[]{ 1 });
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void encryptorThrowsOnZeroEncryptions(CipherSpec cipherSpec) {
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 1);
        assertThatThrownBy(() -> dek.encryptor(0))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void encryptorThrowsOnNegativeEncryptions(CipherSpec cipherSpec) {
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 1);
        assertThatThrownBy(() -> dek.encryptor(-1))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void returnsEdek(CipherSpec cipherSpec) {
        var key = makeKey();
        String edek = "edek";
        Dek<String> dek = new Dek<>(edek, key, cipherSpec, 100);
        assertThat(dek.encryptor(1).edek()).isSameAs(edek);

    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroyUnusedDek(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 100);

        // When
        dek.destroy();

        // Then
        assertThat(key.isDestroyed()).isTrue();

        dek.destroy(); // This should be safe

        assertThatThrownBy(() -> dek.decryptor()).isExactlyInstanceOf(DestroyedDekException.class);
        assertThatThrownBy(() -> dek.encryptor(1)).isExactlyInstanceOf(DestroyedDekException.class);
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Encryptor_destroyThenClose(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 100);
        var cryptor = dek.encryptor(100);

        // When
        dek.destroy();

        // When
        assertThat(key.isDestroyed()).isFalse();

        dek.destroy(); // try destroying again

        cryptor.close();

        assertThat(key.isDestroyed()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Encryptor_closeThenDestroy(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 100);
        var cryptor = dek.encryptor(100);

        cryptor.close();

        // When
        assertThat(key.isDestroyed()).isFalse();

        dek.destroy(); // try destroying again

        cryptor.close();

        assertThat(key.isDestroyed()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy2Encryptor(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 101);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.encryptor(50);

        // When
        dek.destroyForEncrypt();
        dek.destroyForEncrypt();

        // When
        assertThat(key.isDestroyed()).isFalse();

        cryptor1.close();

        assertThat(key.isDestroyed()).isFalse();

        cryptor2.close();

        assertThat(key.isDestroyed()).isFalse(); // because still open for decrypt

        assertThatThrownBy(() -> dek.encryptor(1))
                .isExactlyInstanceOf(DestroyedDekException.class); // can't get a new encryptor though

        dek.destroyForDecrypt(); // this should trigger key destruction

        assertThat(key.isDestroyed()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy2EncryptorMultiClose(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.encryptor(50);

        // When
        dek.destroy();
        dek.destroy();

        // When
        assertThat(key.isDestroyed()).isFalse();

        cryptor1.close();
        cryptor1.close();

        assertThat(key.isDestroyed()).isFalse();

        cryptor2.close();

        assertThat(key.isDestroyed()).isTrue();

        cryptor1.close();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Decryptor_destroyThenClose(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 0);
        var cryptor = dek.decryptor();

        // When
        dek.destroy();

        // When
        assertThat(key.isDestroyed()).isFalse();

        dek.destroy(); // try destroying again

        cryptor.close();

        assertThat(key.isDestroyed()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Decryptor_closeThenDestroy(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 0);
        var cryptor = dek.decryptor();

        // When
        cryptor.close();

        // When
        assertThat(key.isDestroyed()).isFalse();

        dek.destroy();

        assertThat(key.isDestroyed()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy2Decryptor(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 1);
        var cryptor1 = dek.decryptor();
        var cryptor2 = dek.decryptor();

        // When
        dek.destroyForDecrypt();
        dek.destroyForDecrypt();

        // When
        assertThat(key.isDestroyed()).isFalse();

        cryptor1.close();

        assertThat(key.isDestroyed()).isFalse();

        cryptor2.close();

        assertThat(key.isDestroyed()).isFalse(); // still open for encrypt

        assertThatThrownBy(dek::decryptor)
                .isExactlyInstanceOf(DestroyedDekException.class); // can't get a new decryptor

        dek.destroyForEncrypt(); // this should trigger key destruction

        assertThat(key.isDestroyed()).isTrue();

    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy2DecryptorMultiClose(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 0);
        var cryptor1 = dek.decryptor();
        var cryptor2 = dek.decryptor();

        // When
        dek.destroy();
        dek.destroy();

        // When
        assertThat(key.isDestroyed()).isFalse();

        cryptor1.close();
        cryptor1.close();

        assertThat(key.isDestroyed()).isFalse();

        cryptor2.close();

        assertThat(key.isDestroyed()).isTrue();

        cryptor1.close();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Encryptor1Decryptor_destroy(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.decryptor();

        // When
        dek.destroy();
        dek.destroy(); // should be idempotent

        // When
        assertThat(key.isDestroyed()).isFalse(); // because cryptor1 and 2 outstanding

        cryptor1.close();

        assertThat(key.isDestroyed()).isFalse(); // because cryptor2 outstanding

        cryptor2.close();

        assertThat(key.isDestroyed()).isTrue(); // should now be destroyed

        cryptor2.close(); // cryptor.close should be idempotent
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Encryptor1Decryptor_destroyForEncrypt(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.decryptor();

        // When
        dek.destroyForEncrypt();
        dek.destroyForEncrypt(); // should be idempotent

        // When
        assertThat(key.isDestroyed()).isFalse(); // because cryptor1 and 2 outstanding

        cryptor1.close();

        assertThat(key.isDestroyed()).isFalse(); // because cryptor2 outstanding

        cryptor2.close();

        assertThat(key.isDestroyed()).isFalse(); // because only closed for encrypt

        dek.destroyForDecrypt();

        assertThat(key.isDestroyed()).isTrue();

        cryptor2.close();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroy1Encryptor1Decryptor_destroyForDecrypt(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 100);
        var cryptor1 = dek.encryptor(50);
        var cryptor2 = dek.decryptor();

        // When
        dek.destroyForDecrypt();
        dek.destroyForDecrypt(); // should be idempotent

        // When
        assertThat(key.isDestroyed()).isFalse(); // because cryptor1 and 2 outstanding

        cryptor1.close();

        assertThat(key.isDestroyed()).isFalse(); // because cryptor2 outstanding

        cryptor2.close();

        assertThat(key.isDestroyed()).isFalse(); // because only closed for decrypt

        dek.destroyForEncrypt();

        assertThat(key.isDestroyed()).isTrue();

        cryptor2.close();
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void destroyWhen0InitialEncryptions(CipherSpec cipherSpec) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherSpec, 0);

        assertThat(dek.isDestroyed()).isFalse();
        assertThat(key.isDestroyed()).isFalse();

        // When
        dek.destroyForDecrypt();

        // Then: there should be no need to call destroyForEncrypt for the key to be destroyed
        assertThat(dek.isDestroyed()).isTrue();
    }

    // encrypt and decrypt with no AAD
    record EncryptInfo(DestroyableRawSecretKey key,
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
                .isExactlyInstanceOf(DekException.class)
                .cause().isExactlyInstanceOf(AEADBadTagException.class);
    }

    private String decrypt(CipherSpec cipherSpec, ByteBuffer aad, EncryptInfo encryptInfo) {
        var params = encryptInfo.params();
        var ciphertext = encryptInfo.ciphertext();

        var dek = new Dek<>(encryptInfo.key().getEncoded(), encryptInfo.key(), cipherSpec, 1);
        var decryptor = dek.decryptor();

        // Note, we use a common buffer backing both the plaintext and the ciphertext so that we're testing that
        // decrypt() is copy-safe.
        var plaintext = ciphertext.duplicate();

        decryptor.decrypt(ciphertext, aad, params, plaintext);
        plaintext.flip();
        var bytes = new byte[plaintext.limit()];
        plaintext.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private EncryptInfo encrypt(CipherSpec cipherSpec, ByteBuffer aad, String plaintext) throws NoSuchAlgorithmException {
        // Given
        var generator = KeyGenerator.getInstance("AES");
        SecretKey secretKey = generator.generateKey();
        var key = new DestroyableRawSecretKey(secretKey.getAlgorithm(), secretKey.getEncoded());
        // We need a copy of the key, because the key will be destroyed as a side-effect of using up the last encryption
        var safeKey = new DestroyableRawSecretKey(secretKey.getAlgorithm(), key.getEncoded());
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

        // Note, we use a common buffer backing both the plaintext and the ciphertext so that we're testing that
        // encrypt() is copy-safe.
        var commonBuffer = ByteBuffer.allocate(200);
        commonBuffer.put(plaintext.getBytes(StandardCharsets.UTF_8));
        var plaintextBuffer = commonBuffer.duplicate().flip();

        var paramsBuffer = encryptor.generateParameters(size -> commonBuffer.slice());

        // Move the position of the common buffer to the end of the written parameters
        commonBuffer.position(commonBuffer.position() + paramsBuffer.remaining());

        var ciphertextBuffer = encryptor.encrypt(plaintextBuffer,
                aad,
                size -> commonBuffer.slice());

        // TODO assertions on the buffer

        assertThat(key.isDestroyed()).isTrue();

        assertThat(dek.isDestroyed())
                .describedAs("Key should be destroyed when no encryptions left")
                .isTrue();
        assertThat(plaintextBuffer.position())
                .describedAs("Position should be unchanged")
                .isZero();

        // Shouldn't be able to use the Encryptor again
        assertThatThrownBy(() -> encryptor.generateParameters(size -> ByteBuffer.allocate(size)))
                .isExactlyInstanceOf(DekUsageException.class)
                .hasMessage("The Encryptor has no more operations allowed");
        // assertThatThrownBy(() -> encryptor.encrypt(plaintextBuffer,
        // null,
        // size -> ByteBuffer.allocate(size)))
        // .isExactlyInstanceOf(DekUsageException.class)
        // .hasMessage("The Encryptor has no more operations allowed");
        assertThat(plaintextBuffer.position())
                .describedAs("Position should be unchanged")
                .isZero();

        // It should be safe to close the encryptor
        encryptor.close();

        return new EncryptInfo(safeKey, paramsBuffer, ciphertextBuffer);
    }

    // TODO encrypt with too short cipher text buffer
    // TODO encrypt with too short parameters buffer
    // TODO decrypt with wrong IV

}
