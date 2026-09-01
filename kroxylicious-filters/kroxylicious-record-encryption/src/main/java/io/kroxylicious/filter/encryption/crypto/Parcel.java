/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.BiConsumer;

import io.kroxylicious.kafka.common.header.Header;
import io.kroxylicious.kafka.common.record.internal.Record;

import io.kroxylicious.filter.encryption.common.PersistedIdentifiable;
import io.kroxylicious.filter.encryption.config.ParcelVersion;
import io.kroxylicious.filter.encryption.config.RecordField;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Abstraction for constructing the parcel of data which is the plaintext passed to a cipher.
 * What gets included depends on the {@link RecordField}s.
 */
public interface Parcel extends PersistedIdentifiable<ParcelVersion> {
    /**
     * Returns the number of bytes required by {@link #writeParcel(Set, Record, ByteBuffer)}
     * to serialize the parcel for the given record.
     * @param recordFields the fields of the record included in the parcel.
     * @param kafkaRecord the record.
     * @return the number of bytes required to serialize the parcel.
     */
    int sizeOfParcel(@NonNull Set<RecordField> recordFields,
                     @NonNull Record kafkaRecord);

    /**
     * Serializes the parcel for the given record to the given buffer, which should have at least
     * {@link #sizeOfParcel(Set, Record)} bytes {@linkplain ByteBuffer#remaining() remaining}.
     * @param recordFields the fields of the record included in the parcel.
     * @param kafkaRecord the record.
     * @param parcel the buffer to serialize the parcel to.
     */
    void writeParcel(@NonNull Set<RecordField> recordFields,
                     @NonNull Record kafkaRecord,
                     @NonNull ByteBuffer parcel);

    /**
     * Reads a previously-serialized parcel from the given buffer, passing the deserialized record
     * value and headers to the given consumer.
     * @param parcel the buffer to read the parcel from.
     * @param encryptedRecord the encrypted record from which the parcel was decrypted.
     * @param consumer the consumer of the deserialized record value and headers.
     */
    void readParcel(@NonNull ByteBuffer parcel,
                    @NonNull Record encryptedRecord,
                    @NonNull BiConsumer<ByteBuffer, Header[]> consumer);

}
