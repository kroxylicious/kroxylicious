/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import io.kroxylicious.filter.encryption.common.PersistedIdentifiable;
import io.kroxylicious.filter.encryption.config.AadSpec;
import io.kroxylicious.filter.encryption.config.CipherSpec;
import io.kroxylicious.filter.encryption.config.EncryptionVersion;
import io.kroxylicious.filter.encryption.dek.CipherSpecResolver;

/**
 * <p>Behaviour for the version of the encryption in use (as enumerated by {@link EncryptionVersion}).</p>
 *
 * <p>A particular encryption implementation binds together a set of supported ciphers and AADs (which are configurable) and the wrapper and parcel versions used
 * on the encryption path. The idea is that the user defines the version of encryption they want to use (via {@link EncryptionVersion}) and changes that when
 * they want to upgrade. The key point being that the version of the proxy binary is decoupled from the various persistent ids so that newer proxy instances
 * can co-exist with older proxy instances such that the older instances are guaranteed not to be exposed to ciphers, AADs, wrappers or parcels they
 * don't understand (because they lack the code). Once all the proxy instances use using the newer binary the {@link EncryptionVersion} can be bumped.</p>
 */
public class Encryption implements PersistedIdentifiable<EncryptionVersion> {

    public static final Encryption V1 = new Encryption((byte) 1, EncryptionVersion.V1_UNSUPPORTED, WrapperV1.INSTANCE, ParcelV1.INSTANCE);
    public static final Encryption V2 = new Encryption(
            (byte) 2,
            EncryptionVersion.V2,
            new WrapperV2(
                    CipherSpecResolver.of(CipherSpec.AES_256_GCM_128),
                    AadResolver.of(AadSpec.NONE)
            ),
            ParcelV1.INSTANCE
    );
    /***
     * take extreme care when updating the implementations, because new versions are forever once released.
     * If you're adding a new version here you will also need to add it to {@link EncryptionResolver#ALL}.
     ***/

    // TODO in 0.6.0
    // public static Encryption V3 = new Encryption((byte) 2, EncryptionVersion.V2,
    // new WrapperV2(
    // CipherSpecResolver.of(CipherSpec.AES_256_GCM_128, CipherSpec.CHACHA20_POLY1305),
    // AadResolver.of(AadSpec.NONE, AadSpec.BATCH_METADATA)
    // ),
    // ParcelV1.INSTANCE);

    private final byte id;
    private final EncryptionVersion version;
    private final Wrapper wrapper;
    private final Parcel parcel;

    private Encryption(
            byte id,
            EncryptionVersion version,
            Wrapper wrapper,
            Parcel parcel
    ) {
        this.id = id;
        this.version = version;
        this.wrapper = wrapper;
        this.parcel = parcel;
    }

    @Override
    public byte serializedId() {
        return id;
    }

    @Override
    public EncryptionVersion name() {
        return version;
    }

    public Wrapper wrapper() {
        return wrapper;
    }

    public Parcel parcel() {
        return parcel;
    }
}
