/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Utils;

import io.kroxylicious.filter.encryption.common.EncryptionException;
import io.kroxylicious.filter.encryption.config.ParcelVersion;
import io.kroxylicious.filter.encryption.config.RecordField;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class ParcelV1 implements Parcel {

    public static final ParcelV1 INSTANCE = new ParcelV1();

    public static final int NULL_MARKER = -1;
    public static final int ABSENT_MARKER = -2;

    private static final Header[] ABSENT_HEADERS = new Header[0];
    private static final ByteBuffer ABSENT_VALUE = ByteBuffer.allocate(0);

    private ParcelV1() {
    }

    @Override
    public byte serializedId() {
        return 0;
    }

    @Override
    public ParcelVersion name() {
        return ParcelVersion.V1;
    }

    public int sizeOfParcel(@NonNull
    Set<RecordField> recordFields, @NonNull
    Record kafkaRecord) {
        int size = sizeOfRecordValue(recordFields, kafkaRecord);
        size += sizeOfHeaders(recordFields, kafkaRecord);
        return size;
    }

    public void writeParcel(@NonNull
    Set<RecordField> recordFields, @NonNull
    Record kafkaRecord, @NonNull
    ByteBuffer parcel) {
        writeRecordValue(recordFields, kafkaRecord, parcel);
        writeHeaders(recordFields, kafkaRecord, parcel);
    }

    public void readParcel(
            @NonNull
            ByteBuffer parcel,
            @NonNull
            Record encryptedRecord,
            @NonNull
            BiConsumer<ByteBuffer, Header[]> consumer
    ) {
        var parcelledValue = readRecordValue(parcel);
        Header[] parcelledHeaders = readHeaders(parcel);
        var existingHeaders = encryptedRecord.headers();
        Header[] usedHeaders;
        if (parcelledHeaders == ABSENT_HEADERS) {
            if (existingHeaders.length > 0
                && EncryptionHeader.ENCRYPTION_HEADER_NAME.equals(existingHeaders[0].key())) {
                // need to remove the encryption header
                usedHeaders = new Header[existingHeaders.length - 1];
                System.arraycopy(existingHeaders, 1, usedHeaders, 0, usedHeaders.length);
            } else {
                usedHeaders = existingHeaders;
            }
        } else {
            usedHeaders = parcelledHeaders;
        }
        ByteBuffer parcelledBuffer = parcelledValue == ABSENT_VALUE ? encryptedRecord.value() : parcelledValue;
        consumer.accept(parcelledBuffer, usedHeaders);
    }

    private static int sizeOfRecordValue(Set<RecordField> recordFields, Record kafkaRecord) {
        int size = 0;
        if (recordFields.contains(RecordField.RECORD_VALUE)) {
            ByteBuffer value = kafkaRecord.value();
            if (value == null) {
                size += ByteUtils.sizeOfVarint(NULL_MARKER); // value-length
            } else {
                size += ByteUtils.sizeOfVarint(value.limit()); // value-length
                size += value.limit(); // value
            }
        } else {
            size += ByteUtils.sizeOfVarint(ABSENT_MARKER); // value-length
        }
        return size;
    }

    private static void writeRecordValue(Set<RecordField> recordFields, Record kafkaRecord, ByteBuffer parcel) {
        if (recordFields.contains(RecordField.RECORD_VALUE)) {
            ByteBuffer value = kafkaRecord.value();
            if (value == null) {
                ByteUtils.writeVarint(NULL_MARKER, parcel); // value-length
            } else {
                ByteUtils.writeVarint(value.limit(), parcel); // value-length
                parcel.put(value); // value
            }
        } else {
            ByteUtils.writeVarint(ABSENT_MARKER, parcel); // value-length
        }
    }

    private static ByteBuffer readRecordValue(ByteBuffer parcel) {
        var recordValueLength = ByteUtils.readVarint(parcel);
        if (recordValueLength == ABSENT_MARKER) {
            return ABSENT_VALUE;
        } else if (recordValueLength == NULL_MARKER) {
            return null;
        } else if (recordValueLength >= 0) {
            var recordValue = parcel.slice(parcel.position(), recordValueLength);
            parcel.position(parcel.position() + recordValueLength);
            return recordValue;
        } else {
            throw new EncryptionException("Illegal record value length");
        }
    }

    private static int sizeOfHeaders(Set<RecordField> recordFields, Record kafkaRecord) {
        int size;
        if (recordFields.contains(RecordField.RECORD_HEADER_VALUES)) {
            var headers = kafkaRecord.headers();
            size = ByteUtils.sizeOfVarint(headers.length); // headers-length
            for (var header : headers) {
                size += sizeOfHeaderKey(header);
                size += sizeOfHeaderValue(header);
            }
        } else {
            size = ByteUtils.sizeOfVarint(ABSENT_MARKER); // headers-length
        }
        return size;
    }

    private static void writeHeaders(Set<RecordField> recordFields, Record kafkaRecord, ByteBuffer parcel) {
        if (recordFields.contains(RecordField.RECORD_HEADER_VALUES)) {
            var headers = kafkaRecord.headers();
            if (headers == null) {
                ByteUtils.writeVarint(NULL_MARKER, parcel); // headers-length
            } else {
                ByteUtils.writeVarint(headers.length, parcel); // headers-length
                for (var header : headers) {
                    writeHeaderKey(parcel, header);
                    writeHeaderValue(parcel, header);
                }
            }
        } else {
            ByteUtils.writeVarint(ABSENT_MARKER, parcel); // headers-length
        }
    }

    private static Header[] readHeaders(ByteBuffer parcel) {
        var headersLength = ByteUtils.readVarint(parcel);
        if (headersLength == ABSENT_MARKER) {
            return ABSENT_HEADERS;
        } else if (headersLength == NULL_MARKER) {
            return null;
        } else if (headersLength >= 0) {
            Header[] headers = new Header[headersLength];
            for (int i = 0; i < headersLength; i++) {
                headers[i] = readHeader(parcel);
            }
            return headers;
        } else {
            throw new EncryptionException("Illegal headers length");
        }
    }

    @NonNull
    private static RecordHeader readHeader(ByteBuffer parcel) {
        String headerKey = readHeaderKey(parcel);
        byte[] headerValue = readHeaderValue(parcel);
        return new RecordHeader(headerKey, headerValue);
    }

    private static int sizeOfHeaderKey(Header header) {
        int numUtf8Bytes = Utils.utf8Length(header.key());
        int size = ByteUtils.sizeOfUnsignedVarint(numUtf8Bytes); // header-key-length
        size += numUtf8Bytes; // header-key
        return size;
    }

    private static void writeHeaderKey(ByteBuffer parcel, Header header) {
        ByteUtils.writeUnsignedVarint(Utils.utf8Length(header.key()), parcel); // header-key-length
        parcel.put(header.key().getBytes(StandardCharsets.UTF_8)); // header-key
    }

    @NonNull
    private static String readHeaderKey(ByteBuffer parcel) {
        var headerKeyLength = ByteUtils.readUnsignedVarint(parcel);
        String s = Utils.utf8(parcel, headerKeyLength);
        parcel.position(parcel.position() + headerKeyLength);
        return s;
    }

    private static int sizeOfHeaderValue(Header header) {
        byte[] value = header.value();
        int size;
        if (value == null) {
            size = ByteUtils.sizeOfVarint(NULL_MARKER); // header-value-length
        } else {
            size = ByteUtils.sizeOfVarint(value.length); // header-value-length
            size += value.length; // header-value
        }
        return size;
    }

    private static void writeHeaderValue(ByteBuffer parcel, Header header) {
        byte[] value = header.value();
        if (value == null) {
            ByteUtils.writeVarint(NULL_MARKER, parcel); // header-value-length
        } else {
            ByteUtils.writeVarint(value.length, parcel); // header-value-length
            parcel.put(value); // header-value
        }
    }

    @Nullable
    private static byte[] readHeaderValue(ByteBuffer parcel) {
        var headerValueLength = ByteUtils.readVarint(parcel);
        if (headerValueLength == NULL_MARKER) {
            return null;
        } else if (headerValueLength >= 0) {
            byte[] headerValue = new byte[headerValueLength];
            parcel.get(headerValue);
            return headerValue;
        } else {
            throw new EncryptionException("Illegal header value length");
        }
    }
}
