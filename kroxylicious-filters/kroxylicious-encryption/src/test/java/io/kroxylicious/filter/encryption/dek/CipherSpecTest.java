/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.stream.Collectors;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CipherSpecTest {

    @Test
    void fromPersistentIdShouldThrowIfUnknownPersistentId() {
        assertThatThrownBy(() -> CipherSpec.fromPersistentId(123)).isExactlyInstanceOf(UnknownCipherSpecException.class);
    }

    @Test
    void persistentIdsShouldBeUnique() {
        assertThat(Arrays.stream(CipherSpec.values()).map(CipherSpec::persistentId).collect(Collectors.toSet()))
                .hasSize(CipherSpec.values().length);
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void serializedParamsGoodForDecrypt(CipherSpec spec) throws GeneralSecurityException {

        assertThat(CipherSpec.fromPersistentId(spec.persistentId())).isSameAs(spec);

        var params = spec.paramSupplier().get();

        // TODO Right now we're getting away with using AES keys with ChaCha20 cipher
        // (It doesn't work the other way around)
        // Perhaps CipherSpec needs to abstract key generation and key deserialization
        // And the Kms should take the kind of SecretKey as a parameter
        // and use that in how it deserializes keys when generating DekPair and/or unwrapping DEKs
        // Or perhaps it would be more honest for the KMS to return SecretKeySpec
        // ?
        var gen = KeyGenerator.getInstance("AES");
        SecretKey secretKey = gen.generateKey();

        Cipher encCipher = spec.newCipher();
        encCipher.init(Cipher.ENCRYPT_MODE, secretKey, params);
        var ciphertext = encCipher.doFinal("hello, world".getBytes(StandardCharsets.UTF_8));
        int size = spec.size(params);
        assertThat(size).isPositive();
        var bb = ByteBuffer.allocate(size);
        spec.writeParameters(bb, params);
        assertThat(bb.limit())
                .describedAs("Spec should return exact size for parametersBuffer")
                .isEqualTo(size);
        assertThat(bb.position())
                .describedAs("Spec should not do the flip")
                .isEqualTo(bb.limit());

        bb.flip();

        // Prove that we can use the params via serialization to decrypt
        var readParams = spec.readParameters(bb);
        assertThat(bb.position())
                .describedAs("Spec should not do the rewind")
                .isEqualTo(bb.limit());
        Cipher decCipher = spec.newCipher();
        decCipher.init(Cipher.DECRYPT_MODE, secretKey, readParams);
        String plaintext = new String(decCipher.doFinal(ciphertext), StandardCharsets.UTF_8);
        assertThat(plaintext).isEqualTo("hello, world");

    }

}
