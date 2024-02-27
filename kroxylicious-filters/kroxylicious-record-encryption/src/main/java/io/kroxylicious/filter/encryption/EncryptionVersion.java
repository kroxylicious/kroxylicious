/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

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

    V1((byte) 1, ParcelVersion.V1, WrapperVersion.V1);

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
                return V1;
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
