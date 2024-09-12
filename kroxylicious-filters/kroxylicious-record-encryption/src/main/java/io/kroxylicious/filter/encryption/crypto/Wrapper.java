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

import io.kroxylicious.filter.encryption.common.PersistedIdentifiable;
import io.kroxylicious.filter.encryption.config.RecordField;
import io.kroxylicious.filter.encryption.config.WrapperVersion;
import io.kroxylicious.filter.encryption.dek.CipherManager;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Abstraction for the constructing the encrypted wrapper which includes the serialized id of the cipher and its parameters,
 *  any {@link Aad} and the ciphertext of the {@link Parcel}.
 */
public interface Wrapper extends PersistedIdentifiable<WrapperVersion> {
    static <E> ByteBuffer decryptParcel(
            ByteBuffer ciphertextParcel,
            ByteBuffer aad,
            ByteBuffer parameterBuffer,
            Dek<E>.Decryptor encryptor
    ) {
        ByteBuffer plaintext = ciphertextParcel.duplicate();
        encryptor.decrypt(ciphertextParcel, aad, parameterBuffer, plaintext);
        plaintext.flip();
        return plaintext;
    }

    <E> void writeWrapper(
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
    );

    <E> void read(
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
    );

    <E, T> T readSpecAndEdek(
            ByteBuffer wrapper,
            Serde<E> serde,
            BiFunction<CipherManager, E, T> fn
    );
}
