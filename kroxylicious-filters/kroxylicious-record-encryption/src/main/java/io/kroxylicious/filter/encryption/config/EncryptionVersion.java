/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import io.kroxylicious.filter.encryption.common.EncryptionException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The encryption version corresponds directly to a configurable under the proxy administrator's control.
 * The proxy administrator uses this configurable to manage the proxy/filter upgrade process
 * The encryption version directly determines what information is persisted within the encrypted record, and how it is persisted,
 * though the versions of each binary blob used is determined separates (see {@link ParcelVersion}, {@link WrapperVersion}).
 * Other aspects of encryption (such as choice of cipher, or cryptograpihc algorithm provider) correspond to their own
 * admin-visible options.
 */
public enum EncryptionVersion {

    V1_UNSUPPORTED((byte) 1, ParcelVersion.V1, WrapperVersion.V1_UNSUPPORTED),
    V2((byte) 2, ParcelVersion.V1, WrapperVersion.V2);

    private final ParcelVersion parcelVersion;
    private final WrapperVersion wrapperVersion;
    private final byte code;

    EncryptionVersion(byte code, ParcelVersion parcelVersion, WrapperVersion wrapperVersion) {
        this.code = code;
        this.parcelVersion = parcelVersion;
        this.wrapperVersion = wrapperVersion;
    }

    public static EncryptionVersion fromCode(byte code) {
        switch (code) {
            case 1:
                // we guarantee backwards compatibility going forward from v2
                throw new EncryptionException("Deserialization of EncryptionVersion=1 records is not supported by this version of the encryption filter.");
            case 2:
                return V2;
            default:
                throw new EncryptionException("Unknown EncryptionVersion: " + code);
        }
    }

    public @NonNull ParcelVersion parcelVersion() {
        return parcelVersion;
    }

    public @NonNull WrapperVersion wrapperVersion() {
        return wrapperVersion;
    }

    public byte code() {
        return code;
    }
}
