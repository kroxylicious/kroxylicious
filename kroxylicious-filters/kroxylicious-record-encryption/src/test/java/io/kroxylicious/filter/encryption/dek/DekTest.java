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
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kms.service.DestroyableRawSecretKey;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DekTest {

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void constructorThrowsOnDestroyedKey(CipherManager cipherManager) {
        var key = makeKey();
        key.destroy();
        assertThatThrownBy(() -> new Dek<>("edek", key, cipherManager, 100))
                                                                            .isExactlyInstanceOf(IllegalArgumentException.class);

    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void constructorThrowsOnNegativeExceptions(CipherManager cipherManager) {
        var key = makeKey();
        assertThatThrownBy(() -> new Dek<>("edek", key, cipherManager, -1))
                                                                           .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void encryptorThrowsExhaustedDekExceptionOnDekWithZeroEncryptions(CipherManager cipherManager) {
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 0);
        assertThatThrownBy(() -> dek.encryptor(1))
                                                  .isExactlyInstanceOf(ExhaustedDekException.class);
    }

    @NonNull
    private DestroyableRawSecretKey makeKey() {
        return DestroyableRawSecretKey.takeCopyOf(new byte[]{ 1 }, "AES");
    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void encryptorThrowsOnZeroEncryptions(CipherManager cipherManager) {
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 1);
        assertThatThrownBy(() -> dek.encryptor(0))
                                                  .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void encryptorThrowsOnNegativeEncryptions(CipherManager cipherManager) {
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 1);
        assertThatThrownBy(() -> dek.encryptor(-1))
                                                   .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void returnsEdek(CipherManager cipherManager) {
        var key = makeKey();
        String edek = "edek";
        Dek<String> dek = new Dek<>(edek, key, cipherManager, 100);
        assertThat(dek.encryptor(1).edek()).isSameAs(edek);

    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroyUnusedDek(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 100);

        // When
        dek.destroy();

        // Then
        assertThat(key.isDestroyed()).isTrue();

        dek.destroy(); // This should be safe

        assertThatThrownBy(() -> dek.decryptor()).isExactlyInstanceOf(DestroyedDekException.class);
        assertThatThrownBy(() -> dek.encryptor(1)).isExactlyInstanceOf(DestroyedDekException.class);
    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroy1Encryptor_destroyThenClose(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 100);
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
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroy1Encryptor_closeThenDestroy(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 100);
        var cryptor = dek.encryptor(100);

        cryptor.close();

        // When
        assertThat(key.isDestroyed()).isFalse();

        dek.destroy(); // try destroying again

        cryptor.close();

        assertThat(key.isDestroyed()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroy2Encryptor(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 101);
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
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroy2EncryptorMultiClose(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 100);
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
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroy1Decryptor_destroyThenClose(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 0);
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
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroy1Decryptor_closeThenDestroy(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 0);
        var cryptor = dek.decryptor();

        // When
        cryptor.close();

        // When
        assertThat(key.isDestroyed()).isFalse();

        dek.destroy();

        assertThat(key.isDestroyed()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroy2Decryptor(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 1);
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
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroy2DecryptorMultiClose(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 0);
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
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroy1Encryptor1Decryptor_destroy(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 100);
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
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroy1Encryptor1Decryptor_destroyForEncrypt(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 100);
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
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroy1Encryptor1Decryptor_destroyForDecrypt(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 100);
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
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void destroyWhen0InitialEncryptions(CipherManager cipherManager) throws DestroyFailedException {
        // Given
        var key = makeKey();
        var dek = new Dek<>("edek", key, cipherManager, 0);

        assertThat(dek.isDestroyed()).isFalse();
        assertThat(key.isDestroyed()).isFalse();

        // When
        dek.destroyForDecrypt();

        // Then: there should be no need to call destroyForEncrypt for the key to be destroyed
        assertThat(dek.isDestroyed()).isTrue();
    }

    // encrypt and decrypt with no AAD
    record EncryptInfo(
            DestroyableRawSecretKey key,
            ByteBuffer params,
            ByteBuffer ciphertext
    ) {
    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void encryptDecryptNoAad(CipherManager cipherManager) throws NoSuchAlgorithmException {
        ByteBuffer aad = null;
        var encryptInfo = encrypt(cipherManager, aad, "hello, world");
        var roundTripped = decrypt(cipherManager, aad, encryptInfo);
        assertThat(roundTripped).isEqualTo("hello, world");
    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void encryptDecryptWithAad(CipherManager cipherManager) throws NoSuchAlgorithmException {
        ByteBuffer aad = ByteBuffer.wrap(new byte[]{ 42, 56, 89 });
        var encryptInfo = encrypt(cipherManager, aad, "hello, world");
        var roundTripped = decrypt(cipherManager, aad, encryptInfo);
        assertThat(roundTripped).isEqualTo("hello, world");
    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void encryptDecryptWithMismatchingAad(CipherManager cipherManager) throws NoSuchAlgorithmException {
        ByteBuffer encryptAad = ByteBuffer.wrap(new byte[]{ 42, 56, 89 });
        var encryptInfo = encrypt(cipherManager, encryptAad, "hello, world");

        ByteBuffer decryptAad = ByteBuffer.wrap(new byte[]{ 12, 12, 12 });
        assertThatThrownBy(() -> decrypt(cipherManager, decryptAad, encryptInfo))
                                                                                 .isExactlyInstanceOf(DekException.class)
                                                                                 .cause()
                                                                                 .isExactlyInstanceOf(AEADBadTagException.class);
    }

    private String decrypt(CipherManager cipherManager, ByteBuffer aad, EncryptInfo encryptInfo) {
        var params = encryptInfo.params();
        var ciphertext = encryptInfo.ciphertext();

        var dek = new Dek<>(encryptInfo.key().getEncoded(), encryptInfo.key(), cipherManager, 1);
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

    private EncryptInfo encrypt(CipherManager cipherManager, ByteBuffer aad, String plaintext) throws NoSuchAlgorithmException {
        // Given
        var generator = KeyGenerator.getInstance("AES");
        SecretKey secretKey = generator.generateKey();
        var key = DestroyableRawSecretKey.takeOwnershipOf(secretKey.getEncoded(), secretKey.getAlgorithm());
        // We need a copy of the key, because the key will be destroyed as a side-effect of using up the last encryption
        var safeKey = DestroyableRawSecretKey.toDestroyableKey(secretKey);
        var edek = key.getEncoded(); // For this test it doesn't matter than it's not, in fact, encrypted
        var dek = new Dek<>(edek, key, cipherManager, 1);
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

        var ciphertextBuffer = encryptor.encrypt(
                plaintextBuffer,
                aad,
                size -> commonBuffer.slice()
        );

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
