/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteUtils;

import io.kroxylicious.filter.encryption.dek.BufferTooSmallException;
import io.kroxylicious.filter.encryption.dek.CipherSpec;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.filter.encryption.inband.Parcel;
import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The version of the wrapper schema used to persist information in the wrapper.
 */
public enum WrapperVersion {
    /**
     * <pre>
     * wrapper_v1               = cipher_code
     *                            edek_length
     *                            edek
     *                            aead_id
     *                            [ cipher_parameters_length ] ; iff {@link CipherSpec#constantParamsSize()} returns -1
     *                            cipher_parameters
     *                            parcel_ciphertext
     * cipher_id                = OCTET                        ; {@link CipherSpec#persistentId()}
     * edek_length              = 1*OCTET                      ; unsigned VARINT {@link Serde#sizeOf(Object)}
     * edek                     = *OCTET                       ; edek_length bytes {@link Serde#serialize(Object, ByteBuffer)}
     * aead_id                  = OCTET                        ; {@link AadSpec#persistentId()} }
     * cipher_parameters_length = 1*OCTET                      ; unsigned VARINT
     * cipher_parameters        = *OCTET                       ; cipher_parameters_length bytes
     * parcel_ciphertext        = *OCTET                       ; whatever is left in the buffer
     * </pre>
     */
    V1 {

        @Override
        public <E> void writeWrapper(@NonNull Serde<E> edekSerde,
                                     @NonNull E edek,
                                     @NonNull String topicName,
                                     int partitionId,
                                     @NonNull RecordBatch batch,
                                     @NonNull Record kafkaRecord,
                                     @NonNull Dek<E>.Encryptor encryptor,
                                     @NonNull ParcelVersion parcelVersion,
                                     @NonNull AadSpec aadSpec,
                                     @NonNull Set<RecordField> recordFields,
                                     @NonNull ByteBuffer buffer)
                throws BufferTooSmallException {
            try {
                CipherSpec cipherSpec = encryptor.cipherSpec();
                buffer.put(cipherSpec.persistentId());
                short edekSize = (short) edekSerde.sizeOf(edek);
                ByteUtils.writeUnsignedVarint(edekSize, buffer);
                edekSerde.serialize(edek, buffer);
                buffer.put(aadSpec.persistentId());

                ByteBuffer aad = aadSpec.computeAad(topicName, partitionId, batch);

                // Write the parameters
                writeParameters(encryptor, cipherSpec, buffer);

                // Write the parcel of data that will be encrypted (the plaintext)
                var parcelBuffer = buffer.slice();
                Parcel.writeParcel(parcelVersion, recordFields, kafkaRecord, parcelBuffer);
                parcelBuffer.flip();

                // Overwrite the parcel with the cipher text
                var ct = encryptor.encrypt(parcelBuffer,
                        aad,
                        size -> buffer.slice());
                buffer.position(buffer.position() + ct.remaining());
            }
            catch (BufferOverflowException e) {
                throw new BufferTooSmallException();
            }
        }

        private <E> void writeParameters(@NonNull Dek<E>.Encryptor encryptor, CipherSpec cipherSpec, @NonNull ByteBuffer buffer) {
            int paramsSize = cipherSpec.constantParamsSize();
            final ByteBuffer paramsBuffer;
            if (paramsSize == CipherSpec.VARIABLE_SIZE_PARAMETERS) {
                paramsBuffer = encryptor.generateParameters(size -> {
                    ByteBuffer slice = buffer.slice();
                    ByteUtils.writeUnsignedVarint(size, slice);
                    return slice;
                });
            }
            else {
                paramsBuffer = encryptor.generateParameters(size -> buffer.slice());
            }
            buffer.position(buffer.position() + paramsBuffer.limit());
        }

        public <E, T> T readSpecAndEdek(ByteBuffer wrapper, Serde<E> serde, BiFunction<CipherSpec, E, T> function) {
            CipherSpec cipherSpec = CipherSpec.fromPersistentId(wrapper.get());
            var edekLength = ByteUtils.readUnsignedVarint(wrapper);
            ByteBuffer slice = wrapper.slice(wrapper.position(), edekLength);
            E edek = serde.deserialize(slice);
            return function.apply(cipherSpec, edek);
        }

        @Override
        public <E> void read(@NonNull ParcelVersion parcelVersion,
                             @NonNull String topicName,
                             int partition,
                             @NonNull RecordBatch batch,
                             @NonNull Record record,
                             ByteBuffer wrapper,
                             Dek<E>.Decryptor decryptor,
                             @NonNull BiConsumer<ByteBuffer, Header[]> consumer) {
            CipherSpec cipherSpec = CipherSpec.fromPersistentId(wrapper.get());
            var edekLength = ByteUtils.readUnsignedVarint(wrapper);
            wrapper.position(wrapper.position() + edekLength);

            var aadSpec = AadSpec.fromPersistentId(wrapper.get());

            var parametersBuffer = wrapper.slice();
            int parametersSize;
            if (cipherSpec.constantParamsSize() == CipherSpec.VARIABLE_SIZE_PARAMETERS) {
                parametersSize = ByteUtils.readUnsignedVarint(parametersBuffer);
            }
            else {
                parametersSize = cipherSpec.constantParamsSize();
            }
            parametersBuffer.limit(parametersSize);
            var ciphertext = wrapper.position(wrapper.position() + parametersSize).slice();

            ByteBuffer aad = aadSpec.computeAad(topicName, partition, batch);

            ByteBuffer plaintextParcel = decryptParcel(ciphertext, aad, parametersBuffer, decryptor);
            Parcel.readParcel(parcelVersion, plaintextParcel, record, consumer);
        }
    };

    public abstract <E> void writeWrapper(@NonNull Serde<E> edekSerde,
                                          @NonNull E edek,
                                          @NonNull String topicName,
                                          int partitionId,
                                          @NonNull RecordBatch batch,
                                          @NonNull Record kafkaRecord,
                                          @NonNull Dek<E>.Encryptor encryptor,
                                          @NonNull ParcelVersion parcelVersion,
                                          @NonNull AadSpec aadSpec,
                                          @NonNull Set<RecordField> recordFields,
                                          @NonNull ByteBuffer buffer);

    private static <E> ByteBuffer decryptParcel(ByteBuffer ciphertextParcel,
                                                ByteBuffer aad,
                                                ByteBuffer parameterBuffer,
                                                Dek<E>.Decryptor encryptor) {
        ByteBuffer plaintext = ciphertextParcel.duplicate();
        encryptor.decrypt(ciphertextParcel, aad, parameterBuffer, plaintext);
        plaintext.flip();
        return plaintext;
    }

    public abstract <E> void read(@NonNull ParcelVersion parcelVersion,
                                  @NonNull String topicName,
                                  int partition,
                                  @NonNull RecordBatch batch,
                                  @NonNull Record record,
                                  ByteBuffer wrapper,
                                  Dek<E>.Decryptor decryptor,
                                  @NonNull BiConsumer<ByteBuffer, Header[]> consumer);

    public abstract <E, T> T readSpecAndEdek(ByteBuffer wrapper, Serde<E> serde, BiFunction<CipherSpec, E, T> fn);
}
