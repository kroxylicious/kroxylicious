/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;

import io.kroxylicious.filter.encryption.common.PersistedIdentifiable;
import io.kroxylicious.filter.encryption.config.ParcelVersion;
import io.kroxylicious.filter.encryption.config.RecordField;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface Parcel extends PersistedIdentifiable<ParcelVersion> {
    int sizeOfParcel(@NonNull Set<RecordField> recordFields,
                     @NonNull Record kafkaRecord);

    void writeParcel(@NonNull Set<RecordField> recordFields,
                     Record kafkaRecord,
                     @NonNull ByteBuffer parcel);

    void readParcel(@NonNull ByteBuffer parcel,
                    Record encryptedRecord,
                    @NonNull BiConsumer<ByteBuffer, Header[]> consumer);

}
