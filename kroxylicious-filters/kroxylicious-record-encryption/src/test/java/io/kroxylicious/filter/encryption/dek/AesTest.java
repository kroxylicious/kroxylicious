/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.security.Security;
import java.util.Map;

import javax.crypto.Cipher;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.config.CipherOverrideConfig;
import io.kroxylicious.filter.encryption.config.CipherOverrides;
import io.kroxylicious.filter.encryption.config.CipherSpec;
import io.kroxylicious.filter.encryption.config.EncryptionConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AesTest {

    @Test
    void defaultAes256gcm128TransformationEmptyConfig() {
        Aes aes = Aes.aes256gcm128(new CipherOverrideConfig(Map.of()));
        Cipher cipher = aes.newCipher();
        assertThat(cipher.getAlgorithm()).isEqualTo("AES_256/GCM/NoPadding");
    }

    @Test
    void defaultAes256gcm128TransformationNullOverride() {
        Aes aes = Aes.aes256gcm128(new CipherOverrideConfig(Map.of(CipherSpec.AES_256_GCM_128, new CipherOverrides(null, null))));
        Cipher cipher = aes.newCipher();
        assertThat(cipher.getAlgorithm()).isEqualTo("AES_256/GCM/NoPadding");
    }

    @Test
    void overrideAes256gcm128Transformation() {
        Aes aes = Aes.aes256gcm128(new CipherOverrideConfig(Map.of(CipherSpec.AES_256_GCM_128, new CipherOverrides("AES/GCM/NoPadding", null))));
        Cipher cipher = aes.newCipher();
        assertThat(cipher.getAlgorithm()).isEqualTo("AES/GCM/NoPadding");
    }

    @Test
    void cannotOverrideAes256gcm128TransformationToArbitraryAlgorithm() {
        CipherOverrideConfig cipherOverrideConfig = new CipherOverrideConfig(
                Map.of(CipherSpec.AES_256_GCM_128, new CipherOverrides("ChaCha20-Poly1305/NONE/NoPadding", null)));
        assertThatThrownBy(() -> Aes.aes256gcm128(cipherOverrideConfig)).isInstanceOf(EncryptionConfigurationException.class).hasMessage(
                "AES_256_GCM_128 override transformation: ChaCha20-Poly1305/NONE/NoPadding is not one of the allowed values: [AES/GCM/NoPadding, AES_256/GCM/NoPadding]");
    }

    @Test
    void overrideAes256gcm128Provider() {
        BouncyCastleProvider alternativeProvider = new BouncyCastleProvider();
        try {
            Security.addProvider(alternativeProvider);
            Aes aes = Aes
                    .aes256gcm128(new CipherOverrideConfig(Map.of(CipherSpec.AES_256_GCM_128, new CipherOverrides("AES/GCM/NoPadding", alternativeProvider.getName()))));
            Cipher cipher = aes.newCipher();
            assertThat(cipher.getAlgorithm()).isEqualTo("AES/GCM/NoPadding");
            assertThat(cipher.getProvider()).isEqualTo(alternativeProvider);
        }
        finally {
            Security.removeProvider(alternativeProvider.getName());
        }
    }

}
