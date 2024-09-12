/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.config.AadSpec;
import io.kroxylicious.filter.encryption.config.CipherSpec;
import io.kroxylicious.filter.encryption.dek.Aes;
import io.kroxylicious.filter.encryption.dek.UnknownCipherSpecException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EncryptionTest {

    @Test
    void v1() {
        assertThat(Encryption.V1.wrapper()).isExactlyInstanceOf(WrapperV1.class);
        assertThat(Encryption.V1.parcel()).isExactlyInstanceOf(ParcelV1.class);
    }

    @Test
    void v2() {
        // Encryption v1 supports WrapperV2
        assertThat(Encryption.V2.wrapper()).isExactlyInstanceOf(WrapperV2.class);

        // Encryption v1 supports ParcelV1
        assertThat(Encryption.V2.parcel()).isExactlyInstanceOf(ParcelV1.class);

        var cipherSpecResolver = ((WrapperV2) Encryption.V2.wrapper()).cipherSpecResolver();
        // Encryption v1 supports AES, does not support CHACHA
        assertThat(cipherSpecResolver.fromName(CipherSpec.AES_256_GCM_128)).isSameAs(Aes.AES_256_GCM_128);
        assertThat(cipherSpecResolver.fromSerializedId(Aes.AES_256_GCM_128.serializedId())).isSameAs(Aes.AES_256_GCM_128);
        assertThat(cipherSpecResolver.toSerializedId(Aes.AES_256_GCM_128)).isEqualTo(Aes.AES_256_GCM_128.serializedId());
        // Encryption v1 does not support CHACHA
        assertThatThrownBy(() -> cipherSpecResolver.fromName(CipherSpec.CHACHA20_POLY1305)).isExactlyInstanceOf(UnknownCipherSpecException.class)
                                                                                           .hasMessage("Unknown CipherSpec name: CHACHA20_POLY1305");

        var aadResolver = ((WrapperV2) Encryption.V2.wrapper()).aadResolver();
        // Encryption v1 supports AAD.NONE
        assertThat(aadResolver.fromName(AadSpec.NONE)).isExactlyInstanceOf(AadNone.class);
        assertThat(aadResolver.fromSerializedId(AadNone.INSTANCE.serializedId())).isExactlyInstanceOf(AadNone.class);
        assertThat(aadResolver.toSerializedId(AadNone.INSTANCE)).isEqualTo(AadNone.INSTANCE.serializedId());
    }

}
