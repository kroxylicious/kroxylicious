/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.decrypt;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.kafka.transform.RecordTransform;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link RecordTransform} that decrypts records that were previously encrypted by {@link io.kroxylicious.filter.encryption.encrypt.RecordEncryptor}.
 */
public class RecordDecryptor<E> implements RecordTransform<DecryptState<E>> {

    private final String topicName;
    private final int partition;
    private RecordBatch batch;

    private ByteBuffer transformedValue;
    private Header[] transformedHeaders;

    public RecordDecryptor(@NonNull
    String topicName, int partition) {
        this.topicName = Objects.requireNonNull(topicName);
        this.partition = partition;
    }

    @Override
    public void initBatch(@NonNull
    RecordBatch batch) {
        this.batch = Objects.requireNonNull(batch);
    }

    @Override
    public void init(
            @Nullable
            DecryptState<E> decryptState,
            @NonNull
            Record record
    ) {
        if (batch == null) {
            throw new IllegalStateException();
        }
        final Dek<E>.Decryptor decryptor;
        if (decryptState == null) {
            decryptor = null;
        } else {
            decryptor = decryptState.decryptor();
        }
        if (decryptor == null) {
            transformedValue = record.value();
            transformedHeaders = record.headers();
            return;
        }

        var wrapper = record.value();
        decryptState.encryptionUsed()
                    .wrapper()
                    .read(
                            decryptState.encryptionUsed().parcel(),
                            topicName,
                            partition,
                            batch,
                            record,
                            wrapper,
                            decryptor,
                            (v, h) -> {
                                transformedValue = v;
                                transformedHeaders = h;
                            }
                    );

    }

    @Override
    public long transformOffset(@NonNull
    Record record) {
        return record.offset();
    }

    @Override
    public long transformTimestamp(@NonNull
    Record record) {
        return record.timestamp();
    }

    @Nullable
    @Override
    public ByteBuffer transformKey(@NonNull
    Record record) {
        return record.key();
    }

    @Nullable
    @Override
    public ByteBuffer transformValue(@NonNull
    Record record) {
        return transformedValue == null ? null : transformedValue.duplicate();
    }

    @Nullable
    @Override
    public Header[] transformHeaders(@NonNull
    Record record) {
        return transformedHeaders;
    }

    @Override
    public void resetAfterTransform(
            @Nullable
            DecryptState decryptState,
            @NonNull
            Record record
    ) {
        if (transformedValue != null) {
            transformedValue.clear();
        }
        transformedHeaders = null;
    }
}
