/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteUtils;

import io.kroxylicious.filter.encryption.AadSpec;
import io.kroxylicious.filter.encryption.CipherCode;
import io.kroxylicious.filter.encryption.EncryptionScheme;
import io.kroxylicious.filter.encryption.EncryptionVersion;
import io.kroxylicious.filter.encryption.RecordField;
import io.kroxylicious.filter.encryption.records.RecordTransform;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link RecordTransform} that encrypts records so that they can be later decrypted by {@link RecordDecryptor}.
 * @param <K> The type of KEK id
 */
class RecordEncryptor<K> implements RecordTransform {

    /**
     * The encryption header. The value is the encryption version that was used to serialize the parcel and the wrapper.
     */
    static final String ENCRYPTION_HEADER_NAME = "kroxylicious.io/encryption";
    private final EncryptionVersion encryptionVersion;
    private final EncryptionScheme<K> encryptionScheme;
    private final KeyContext keyContext;
    private final ByteBuffer parcelBuffer;
    private final ByteBuffer wrapperBuffer;
    /**
     * The encryption version used on the produce path.
     * Note that the encryption version used on the fetch path is read from the
     * {@link #ENCRYPTION_HEADER_NAME} header.
     */
    private final Header[] encryptionHeader;
    private @Nullable ByteBuffer transformedValue;
    private @Nullable Header[] transformedHeaders;

    /**
     * Constructor (obviously).
     * @param encryptionVersion The encryption version
     * @param encryptionScheme The encryption scheme for this key
     * @param keyContext The key context
     * @param parcelBuffer A buffer big enough to write the parcel
     * @param wrapperBuffer A buffer big enough to write the wrapper
     */
    RecordEncryptor(@NonNull EncryptionVersion encryptionVersion,
                    @NonNull EncryptionScheme<K> encryptionScheme,
                    @NonNull KeyContext keyContext,
                    @NonNull ByteBuffer parcelBuffer,
                    @NonNull ByteBuffer wrapperBuffer) {
        Objects.requireNonNull(encryptionVersion);
        Objects.requireNonNull(encryptionScheme);
        Objects.requireNonNull(keyContext);
        Objects.requireNonNull(parcelBuffer);
        Objects.requireNonNull(wrapperBuffer);
        this.encryptionVersion = encryptionVersion;
        this.encryptionScheme = encryptionScheme;
        this.keyContext = keyContext;
        this.parcelBuffer = parcelBuffer;
        this.wrapperBuffer = wrapperBuffer;
        this.encryptionHeader = new Header[]{ new RecordHeader(ENCRYPTION_HEADER_NAME, new byte[]{ encryptionVersion.code() }) };
    }

    @Override
    public void init(@NonNull Record kafkaRecord) {
        if (encryptionScheme.recordFields().contains(RecordField.RECORD_HEADER_VALUES)
                && kafkaRecord.headers().length > 0
                && !kafkaRecord.hasValue()) {
            // todo implement header encryption preserving null record-values
            throw new IllegalStateException("encrypting headers prohibited when original record value null, we must preserve the null for tombstoning");
        }

        this.transformedValue = doTransformValue(kafkaRecord);
        this.transformedHeaders = doTransformHeaders(kafkaRecord);
    }

    @Nullable
    private ByteBuffer doTransformValue(@NonNull Record kafkaRecord) {
        final ByteBuffer transformedValue;
        if (kafkaRecord.hasValue()) {
            Parcel.writeParcel(encryptionVersion.parcelVersion(), encryptionScheme.recordFields(), kafkaRecord, parcelBuffer);
            parcelBuffer.flip();
            transformedValue = writeWrapper(parcelBuffer);
            parcelBuffer.rewind();
        }
        else {
            transformedValue = null;
        }
        return transformedValue;
    }

    private Header[] doTransformHeaders(@NonNull Record kafkaRecord) {
        final Header[] transformedHeaders;
        if (kafkaRecord.hasValue()) {
            Header[] oldHeaders = kafkaRecord.headers();
            if (encryptionScheme.recordFields().contains(RecordField.RECORD_HEADER_VALUES) || oldHeaders.length == 0) {
                transformedHeaders = encryptionHeader;
            }
            else {
                transformedHeaders = new Header[1 + oldHeaders.length];
                transformedHeaders[0] = encryptionHeader[0];
                System.arraycopy(oldHeaders, 0, transformedHeaders, 1, oldHeaders.length);
            }
        }
        else {
            transformedHeaders = kafkaRecord.headers();
        }
        return transformedHeaders;
    }

    @Nullable
    private ByteBuffer writeWrapper(ByteBuffer parcelBuffer) {
        switch (encryptionVersion.wrapperVersion()) {
            case V1 -> {
                var edek = keyContext.serializedEdek();
                ByteUtils.writeUnsignedVarint(edek.length, wrapperBuffer);
                wrapperBuffer.put(edek);
                wrapperBuffer.put(AadSpec.NONE.code()); // aadCode
                wrapperBuffer.put(CipherCode.AES_GCM_96_128.code());
                keyContext.encodedSize(parcelBuffer.limit());
                ByteBuffer aad = ByteUtils.EMPTY_BUF; // TODO pass the AAD to encode
                keyContext.encode(parcelBuffer, wrapperBuffer); // iv and ciphertext
            }
        }
        wrapperBuffer.flip();
        return wrapperBuffer;
    }

    @Override
    public void resetAfterTransform(Record record) {
        wrapperBuffer.rewind();
    }

    @Override
    public long transformOffset(Record record) {
        return record.offset();
    }

    @Override
    public long transformTimestamp(Record record) {
        return record.timestamp();
    }

    @Override
    public @Nullable ByteBuffer transformKey(Record record) {
        return record.key();
    }

    @Override
    public @Nullable ByteBuffer transformValue(Record kafkaRecord) {
        return transformedValue == null ? null : transformedValue.duplicate();
    }

    @Override
    public @Nullable Header[] transformHeaders(Record kafkaRecord) {
        return transformedHeaders;
    }

}
