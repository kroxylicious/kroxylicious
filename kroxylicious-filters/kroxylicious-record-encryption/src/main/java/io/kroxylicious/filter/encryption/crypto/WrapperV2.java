/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteUtils;

import io.kroxylicious.filter.encryption.common.EncryptionException;
import io.kroxylicious.filter.encryption.config.RecordField;
import io.kroxylicious.filter.encryption.config.WrapperVersion;
import io.kroxylicious.filter.encryption.dek.BufferTooSmallException;
import io.kroxylicious.filter.encryption.dek.CipherManager;
import io.kroxylicious.filter.encryption.dek.CipherSpecResolver;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * <pre>
 * wrapper_v2               = cipher_id
 *                            edek_length
 *                            edek
 *                            aad_id
 *                            [ cipher_parameters_length ] ; iff {@link CipherManager#constantParamsSize()} returns -1
 *                            cipher_parameters
 *                            parcel_ciphertext
 * cipher_id                = OCTET                        ; {@link CipherSpecResolver#toSerializedId(PersistedIdentifiable)}}
 * edek_length              = 1*OCTET                      ; unsigned VARINT {@link Serde#sizeOf(Object)}
 * edek                     = *OCTET                       ; edek_length bytes {@link Serde#serialize(Object, ByteBuffer)}
 * aad_id                   = OCTET                        ; {@link AadResolver#toSerializedId}
 * cipher_parameters_length = 1*OCTET                      ; unsigned VARINT
 * cipher_parameters        = *OCTET                       ; cipher_parameters_length bytes
 * parcel_ciphertext        = *OCTET                       ; whatever is left in the buffer
 * </pre>
 */
public class WrapperV2 implements Wrapper {

    public final CipherSpecResolver cipherSpecResolver;
    public final AadResolver aadResolver;

    public WrapperV2(CipherSpecResolver cipherSpecResolver,
                     AadResolver aadResolver) {
        this.cipherSpecResolver = cipherSpecResolver;
        this.aadResolver = aadResolver;
    }

    @VisibleForTesting
    CipherSpecResolver cipherSpecResolver() {
        return cipherSpecResolver;
    }

    @VisibleForTesting
    AadResolver aadResolver() {
        return aadResolver;
    }

    @Override
    public byte serializedId() {
        return 1;
    }

    @Override
    public WrapperVersion name() {
        return WrapperVersion.V2;
    }

    @Override
    public <E> void writeWrapper(@NonNull Serde<E> edekSerde,
                                 @NonNull E edek,
                                 @NonNull String topicName,
                                 int partitionId,
                                 @NonNull RecordBatch batch,
                                 @NonNull Record kafkaRecord,
                                 @NonNull Dek<E>.Encryptor encryptor,
                                 @NonNull Parcel parcel,
                                 @NonNull Aad aad,
                                 @NonNull Set<RecordField> recordFields,
                                 @NonNull ByteBuffer buffer)
            throws BufferTooSmallException {
        try {
            CipherManager cipherManager = encryptor.cipherManager();
            buffer.put(cipherSpecResolver.toSerializedId(cipherManager));
            short edekSize = (short) edekSerde.sizeOf(edek);
            ByteUtils.writeUnsignedVarint(edekSize, buffer);
            edekSerde.serialize(edek, buffer);
            buffer.put(aadResolver.toSerializedId(aad));

            ByteBuffer aadBuffer = aad.computeAad(topicName, partitionId, batch);

            // Write the parameters
            writeParameters(encryptor, cipherManager, buffer);

            // Write the parcel of data that will be encrypted (the plaintext)
            var parcelBuffer = buffer.slice();
            parcel.writeParcel(recordFields, kafkaRecord, parcelBuffer);
            parcelBuffer.flip();

            // Overwrite the parcel with the cipher text
            var ct = encryptor.encrypt(parcelBuffer,
                    aadBuffer,
                    size -> buffer.slice());
            buffer.position(buffer.position() + ct.remaining());
        }
        catch (BufferOverflowException e) {
            encryptor.recredit();
            throw new BufferTooSmallException();
        }
    }

    private <E> void writeParameters(@NonNull Dek<E>.Encryptor encryptor, CipherManager cipherManager, @NonNull ByteBuffer buffer) {
        int paramsSize = cipherManager.constantParamsSize();
        final ByteBuffer paramsBuffer;
        if (paramsSize == CipherManager.VARIABLE_SIZE_PARAMETERS) {
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

    public <E, T> T readSpecAndEdek(ByteBuffer wrapper, Serde<E> serde, BiFunction<CipherManager, E, T> function) {
        CipherManager cipherManager = cipherSpecResolver.fromSerializedId(wrapper.get());
        var edekLength = ByteUtils.readUnsignedVarint(wrapper);
        ByteBuffer slice = wrapper.slice(wrapper.position(), edekLength);
        E edek = serde.deserialize(slice);
        return function.apply(cipherManager, edek);
    }

    @Override
    public <E> void read(@NonNull Parcel parcel,
                         @NonNull String topicName,
                         int partition,
                         @NonNull RecordBatch batch,
                         @NonNull Record record,
                         ByteBuffer wrapper,
                         Dek<E>.Decryptor decryptor,
                         @NonNull BiConsumer<ByteBuffer, Header[]> consumer) {
        CipherManager cipherManager = cipherSpecResolver.fromSerializedId(wrapper.get());
        var edekLength = ByteUtils.readUnsignedVarint(wrapper);
        wrapper.position(wrapper.position() + edekLength);

        var aadSpec = aadResolver.fromSerializedId(wrapper.get());

        var parametersBuffer = wrapper.slice();
        if (cipherManager.constantParamsSize() == CipherManager.VARIABLE_SIZE_PARAMETERS) {
            // when we implement this we need to read parameterSize from the varint and
            // ensure the parametersBuffer limit includes the length of the varint and
            // ensure we include the length of the varint when skipping over the parameters in the wrapper
            throw new EncryptionException("variable size cipher parameters not supported yet");
        }
        int parametersSize = cipherManager.constantParamsSize();
        parametersBuffer.limit(parametersSize);
        var ciphertext = wrapper.position(wrapper.position() + parametersSize).slice();

        ByteBuffer aad = aadSpec.computeAad(topicName, partition, batch);

        ByteBuffer plaintextParcel = Wrapper.decryptParcel(ciphertext, aad, parametersBuffer, decryptor);
        parcel.readParcel(plaintextParcel, record, consumer);
    }
}
