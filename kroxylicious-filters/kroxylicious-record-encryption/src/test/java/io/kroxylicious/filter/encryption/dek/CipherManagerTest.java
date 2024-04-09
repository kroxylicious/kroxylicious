/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.kroxylicious.filter.encryption.config.CipherSpec;

import static org.assertj.core.api.Assertions.assertThat;

class CipherManagerTest {

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void serializedParamsGoodForDecrypt(CipherSpec spec) throws GeneralSecurityException {

        var cipherManager = CipherSpecResolver.INSTANCE.fromSpec(spec);

        assertThat(CipherSpecResolver.INSTANCE.fromPersistentId(
                CipherSpecResolver.INSTANCE.persistentId(cipherManager))).isSameAs(cipherManager);

        var params = cipherManager.paramSupplier().get();

        // TODO Right now we're getting away with using AES keys with ChaCha20 cipher
        // (It doesn't work the other way around)
        // Perhaps CipherSpec needs to abstract key generation and key deserialization
        // And the Kms should take the kind of SecretKey as a parameter
        // and use that in how it deserializes keys when generating DekPair and/or unwrapping DEKs
        // Or perhaps it would be more honest for the KMS to return SecretKeySpec
        // ?
        var gen = KeyGenerator.getInstance("AES");
        SecretKey secretKey = gen.generateKey();

        Cipher encCipher = cipherManager.newCipher();
        encCipher.init(Cipher.ENCRYPT_MODE, secretKey, params);
        var ciphertext = encCipher.doFinal("hello, world".getBytes(StandardCharsets.UTF_8));
        int size = cipherManager.size(params);
        assertThat(size).isPositive();
        var bb = ByteBuffer.allocate(size);
        cipherManager.writeParameters(bb, params);
        assertThat(bb.limit())
                .describedAs("Spec should return exact size for parametersBuffer")
                .isEqualTo(size);
        assertThat(bb.position())
                .describedAs("Spec should not do the flip")
                .isEqualTo(bb.limit());

        bb.flip();

        // Prove that we can use the params via serialization to decrypt
        var readParams = cipherManager.readParameters(bb);
        assertThat(bb.position())
                .describedAs("Spec should not do the rewind")
                .isEqualTo(bb.limit());
        Cipher decCipher = cipherManager.newCipher();
        decCipher.init(Cipher.DECRYPT_MODE, secretKey, readParams);
        String plaintext = new String(decCipher.doFinal(ciphertext), StandardCharsets.UTF_8);
        assertThat(plaintext).isEqualTo("hello, world");

    }

}
