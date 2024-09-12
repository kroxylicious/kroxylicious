/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import io.kroxylicious.filter.encryption.common.EncryptionException;
import io.kroxylicious.filter.encryption.config.RecordField;
import io.kroxylicious.filter.encryption.config.WrapperVersion;
import io.kroxylicious.filter.encryption.dek.CipherManager;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

public class WrapperV1 implements Wrapper {

    public static final WrapperV1 INSTANCE = new WrapperV1();

    @Override
    public byte serializedId() {
        return 0;
    }

    @Override
    public WrapperVersion name() {
        return WrapperVersion.V1_UNSUPPORTED;
    }

    @Override
    public <E> void writeWrapper(
            @NonNull
            Serde<E> edekSerde,
            @NonNull
            E edek,
            @NonNull
            String topicName,
            int partitionId,
            @NonNull
            RecordBatch batch,
            @NonNull
            Record kafkaRecord,
            @NonNull
            Dek<E>.Encryptor encryptor,
            @NonNull
            Parcel parcel,
            @NonNull
            Aad aadSpec,
            @NonNull
            Set<RecordField> recordFields,
            @NonNull
            ByteBuffer buffer
    ) {
        throw unsupportedVersionException();
    }

    @Override
    public <E> void read(
            @NonNull
            Parcel parcel,
            @NonNull
            String topicName,
            int partition,
            @NonNull
            RecordBatch batch,
            @NonNull
            Record record,
            ByteBuffer wrapper,
            Dek<E>.Decryptor decryptor,
            @NonNull
            BiConsumer<ByteBuffer, Header[]> consumer
    ) {
        throw unsupportedVersionException();
    }

    @Override
    public <E, T> T readSpecAndEdek(ByteBuffer wrapper, Serde<E> serde, BiFunction<CipherManager, E, T> fn) {
        throw unsupportedVersionException();
    }

    @NonNull
    private static EncryptionException unsupportedVersionException() {
        return new EncryptionException("V1 wrappers are unsupported");
    }
}
