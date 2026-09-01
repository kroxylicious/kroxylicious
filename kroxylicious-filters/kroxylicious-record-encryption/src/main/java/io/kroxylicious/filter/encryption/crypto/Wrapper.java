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

import io.kroxylicious.kafka.common.header.Header;
import io.kroxylicious.kafka.common.record.internal.Record;
import io.kroxylicious.kafka.common.record.internal.RecordBatch;

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
    /**
     * Decrypts a parcel in place, returning a buffer containing the plaintext parcel.
     * @param ciphertextParcel the buffer containing the ciphertext of the parcel.
     * @param aad the AAD to be verified as part of the decryption.
     * @param parameterBuffer the buffer containing the cipher parameters.
     * @param encryptor the decryptor to be used to decrypt the parcel.
     * @param <E> the type of encrypted DEK.
     * @return a buffer containing the plaintext parcel.
     */
    static <E> ByteBuffer decryptParcel(
                                        ByteBuffer ciphertextParcel,
                                        ByteBuffer aad,
                                        ByteBuffer parameterBuffer,
                                        Dek<E>.Decryptor encryptor) {
        ByteBuffer plaintext = ciphertextParcel.duplicate();
        encryptor.decrypt(ciphertextParcel, aad, parameterBuffer, plaintext);
        plaintext.flip();
        return plaintext;
    }

    /**
     * Serializes the wrapper for the given record to the given buffer, encrypting the record's parcel.
     * @param edekSerde the serde for the encrypted DEK.
     * @param edek the encrypted DEK.
     * @param topicName the name of the topic to which the record is being produced.
     * @param partitionId the index of the partition to which the record is being produced.
     * @param batch the batch containing the record.
     * @param kafkaRecord the record.
     * @param encryptor the encryptor to be used to encrypt the parcel.
     * @param parcel the parcel used to serialize the record's fields.
     * @param aadSpec the AAD to be included in the encryption.
     * @param recordFields the fields of the record included in the parcel.
     * @param buffer the buffer to serialize the wrapper to.
     * @param <E> the type of encrypted DEK.
     */
    <E> void writeWrapper(
                          @NonNull Serde<E> edekSerde,
                          @NonNull E edek,
                          @NonNull String topicName,
                          int partitionId,
                          @NonNull RecordBatch batch,
                          @NonNull Record kafkaRecord,
                          @NonNull Dek<E>.Encryptor encryptor,
                          @NonNull Parcel parcel,
                          @NonNull Aad aadSpec,
                          @NonNull Set<RecordField> recordFields,
                          @NonNull ByteBuffer buffer);

    /**
     * Reads a previously-serialized wrapper from the given buffer, decrypting the parcel and passing
     * the deserialized record value and headers to the given consumer.
     * @param parcel the parcel used to deserialize the record's fields.
     * @param topicName the name of the topic from which the record is being fetched.
     * @param partition the index of the partition from which the record is being fetched.
     * @param batch the batch containing the record.
     * @param record the encrypted record.
     * @param wrapper the buffer to read the wrapper from.
     * @param decryptor the decryptor to be used to decrypt the parcel.
     * @param consumer the consumer of the deserialized record value and headers.
     * @param <E> the type of encrypted DEK.
     */
    <E> void read(
                  @NonNull Parcel parcel,
                  @NonNull String topicName,
                  int partition,
                  @NonNull RecordBatch batch,
                  @NonNull Record record,
                  ByteBuffer wrapper,
                  Dek<E>.Decryptor decryptor,
                  @NonNull BiConsumer<ByteBuffer, Header[]> consumer);

    /**
     * Reads the cipher manager and encrypted DEK from a previously-serialized wrapper,
     * applying the given function to them and returning the result.
     * @param wrapper the buffer to read the wrapper from.
     * @param serde the serde for the encrypted DEK.
     * @param fn the function applied to the cipher manager and encrypted DEK.
     * @param <E> the type of encrypted DEK.
     * @param <T> the type returned by the function.
     * @return the result of applying the function.
     */
    <E, T> T readSpecAndEdek(
                             ByteBuffer wrapper,
                             Serde<E> serde,
                             BiFunction<CipherManager, E, T> fn);
}
