/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.common;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

import edu.umd.cs.findbugs.annotations.NonNull;

public class RecordEncryptionUtil {

    private RecordEncryptionUtil() {
    }

    @SuppressWarnings("unchecked")
    public static <T> CompletionStage<List<T>> join(List<? extends CompletionStage<T>> stages) {
        CompletableFuture<T>[] futures = stages.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures)
                                .thenApply(ignored -> Stream.of(futures).map(CompletableFuture::join).toList());
    }

    public static int totalRecordsInBatches(@NonNull
    MemoryRecords records) {
        int totalRecords = 0;
        for (MutableRecordBatch batch : records.batches()) {
            totalRecords += recordCount(batch);
        }
        return totalRecords;
    }

    private static int recordCount(@NonNull
    MutableRecordBatch batch) {
        Integer count = batch.countOrNull();
        if (count == null) {
            // for magic <2 count will be null
            try (CloseableIterator<Record> iterator = batch.skipKeyValueIterator(BufferSupplier.NO_CACHING)) {
                int c = 0;
                while (iterator.hasNext()) {
                    c++;
                    iterator.next();
                }
                count = c;
            }
        }
        return count;
    }
}
