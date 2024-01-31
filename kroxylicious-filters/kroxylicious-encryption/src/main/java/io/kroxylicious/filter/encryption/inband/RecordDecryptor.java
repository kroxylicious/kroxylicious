/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.IntFunction;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteUtils;

import io.kroxylicious.filter.encryption.AadSpec;
import io.kroxylicious.filter.encryption.CipherCode;
import io.kroxylicious.filter.encryption.records.RecordTransform;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link RecordTransform} that decrypts records that were previously encrypted by {@link RecordEncryptor}.
 */
public class RecordDecryptor implements RecordTransform {

    private final IntFunction<DecryptState> keys;
    int index = 0;
    private ByteBuffer transformedValue;
    private Header[] transformedHeaders;

    public RecordDecryptor(@NonNull IntFunction<DecryptState> keys) {
        Objects.requireNonNull(keys);
        this.keys = keys;
    }

    @Override
    public void init(@NonNull Record record) {
        DecryptState decryptState = keys.apply(index);
        if (decryptState == null || decryptState.encryptor() == null) {
            transformedValue = record.value();
            transformedHeaders = record.headers();
            return;
        }
        var encryptor = decryptState.encryptor();
        var decryptionVersion = decryptState.decryptionVersion();
        var wrapper = record.value();
        // Skip the edek
        var edekLength = ByteUtils.readUnsignedVarint(wrapper);
        wrapper.position(wrapper.position() + edekLength);

        var aadSpec = AadSpec.fromCode(wrapper.get());
        ByteBuffer aad = switch (aadSpec) {
            case NONE -> ByteUtils.EMPTY_BUF;
        };

        var cipherCode = CipherCode.fromCode(wrapper.get());

        ByteBuffer plaintextParcel;
        synchronized (encryptor) {
            plaintextParcel = decryptParcel(wrapper.slice(), encryptor);
        }
        Parcel.readParcel(decryptionVersion.parcelVersion(), plaintextParcel, record, (v, h) -> {
            transformedValue = v;
            transformedHeaders = h;
        });

    }

    private ByteBuffer decryptParcel(ByteBuffer ciphertextParcel, AesGcmEncryptor encryptor) {
        ByteBuffer plaintext = ciphertextParcel.duplicate();
        encryptor.decrypt(ciphertextParcel, plaintext);
        plaintext.flip();
        return plaintext;
    }

    @Override
    public long transformOffset(@NonNull Record record) {
        return record.offset();
    }

    @Override
    public long transformTimestamp(@NonNull Record record) {
        return record.timestamp();
    }

    @Nullable
    @Override
    public ByteBuffer transformKey(@NonNull Record record) {
        return record.key();
    }

    @Nullable
    @Override
    public ByteBuffer transformValue(@NonNull Record record) {
        return transformedValue == null ? null : transformedValue.duplicate();
    }

    @Nullable
    @Override
    public Header[] transformHeaders(@NonNull Record record) {
        return transformedHeaders;
    }

    @Override
    public void resetAfterTransform(@NonNull Record record) {
        if (transformedValue != null) {
            transformedValue.clear();
        }
        transformedHeaders = null;
        index++;
    }
}
